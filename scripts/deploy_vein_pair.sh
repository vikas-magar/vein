#!/bin/bash
# deploy_vein_pair.sh — Deploys Receiver then Sender, wiring the IP automatically
set -e

REGION="ap-south-2"
KEY_NAME="hermit-key"
KEY_FILE="../config/hermit-key.pem"

# ── Helper: get factory instance ─────────────────────────────────────────────
INSTANCE_ID=$(aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:Name,Values=kernel-builder" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].InstanceId" --output text)
PUBLIC_IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" --output text)

echo "═══════════════════════════════════════════════════════════"
echo "  Factory: $INSTANCE_ID @ $PUBLIC_IP"
echo "═══════════════════════════════════════════════════════════"

# ── Helper: get/create EBS deploy volume ─────────────────────────────────────
get_or_create_vol() {
    local TAG=$1
    VOL_ID=$(aws ec2 describe-volumes --region "$REGION" \
        --filters "Name=attachment.instance-id,Values=$INSTANCE_ID" "Name=tag:Name,Values=$TAG" \
        --query "Volumes[0].VolumeId" --output text)
    if [ "$VOL_ID" == "None" ] || [ -z "$VOL_ID" ]; then
        AVAIL_ZONE=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
            --query "Reservations[0].Instances[0].Placement.AvailabilityZone" --output text)
        VOL_ID=$(aws ec2 create-volume --region "$REGION" --availability-zone "$AVAIL_ZONE" \
            --size 1 --volume-type gp3 \
            --tag-specifications "ResourceType=volume,Tags=[{Key=Name,Value=$TAG}]" \
            --query "VolumeId" --output text)
        aws ec2 wait volume-available --region "$REGION" --volume-ids "$VOL_ID"
        
        # Determine next available block device (/dev/sdf, /dev/sdg, etc)
        # For simplicity, if it's the sender, use /dev/sdg, otherwise /dev/sdf
        if [[ "$TAG" == *"sender"* ]]; then
            aws ec2 attach-volume --region "$REGION" --volume-id "$VOL_ID" \
                --instance-id "$INSTANCE_ID" --device /dev/sdg > /dev/null
        else
            aws ec2 attach-volume --region "$REGION" --volume-id "$VOL_ID" \
                --instance-id "$INSTANCE_ID" --device /dev/sdf > /dev/null
        fi
        # Wait until volume attachment is fully 'attached' (not just 'in-use')
        aws ec2 wait volume-in-use --region "$REGION" --volume-ids "$VOL_ID" \
            --filters "Name=attachment.status,Values=attached"
        sleep 2
    fi
    echo "$VOL_ID"
}

# ── Helper: push workspace ────────────────────────────────────────────────────
push_workspace() {
    ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" ubuntu@"$PUBLIC_IP" \
        'mkdir -p /home/ubuntu/vein_workspace'
    rsync -az --exclude='target/' -e "ssh -o StrictHostKeyChecking=no -i $KEY_FILE" \
        ../crates ../Cargo.toml ../certs ubuntu@$PUBLIC_IP:/home/ubuntu/vein_workspace/
    rsync -az -e "ssh -o StrictHostKeyChecking=no -i $KEY_FILE" \
        ../linux/Image ubuntu@$PUBLIC_IP:/home/ubuntu/
}

remote_build_and_write() {
    local BIN=$1
    local EXTRA_FILE=$2   # optional: "path:content" to inject into initramfs
    local BLK_DEV=$3
    ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" ubuntu@"$PUBLIC_IP" "bash -s '$BIN' '$EXTRA_FILE' '$BLK_DEV'" << 'REMOTE_EOF'
BIN=$1
EXTRA_FILE=$2
BLK_DEV=$3
set -e

export CC_aarch64_unknown_linux_musl=musl-gcc

sudo apt-get update -q && sudo apt-get install -q -y cmake clang llvm musl-tools build-essential
if [ ! -f "$HOME/.cargo/env" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
    rustup target add aarch64-unknown-linux-musl
else
    source $HOME/.cargo/env
fi

echo "[REMOTE] Compiling $BIN..."
cd vein_workspace/crates/$BIN
cargo fetch
# Apply sed fix for s2n-quic-xdp if it exists in the registry
sed -i 's/libc::SIOCETHTOOL, \&mut ifreq/libc::SIOCETHTOOL.try_into().unwrap(), \&mut ifreq/g' $HOME/.cargo/registry/src/*/s2n-quic-xdp-*/src/syscall.rs || true
cargo build --target aarch64-unknown-linux-musl --release
cd ../../..

rm -rf custom_ramdisk
mkdir -p custom_ramdisk
if [ -f "vein_workspace/target/aarch64-unknown-linux-musl/release/$BIN" ]; then
    cp vein_workspace/target/aarch64-unknown-linux-musl/release/$BIN custom_ramdisk/init
else
    cp vein_workspace/crates/$BIN/target/aarch64-unknown-linux-musl/release/$BIN custom_ramdisk/init
fi
chmod +x custom_ramdisk/init

# Inject optional extra file (e.g., receiver_ip)
if [ -n "$EXTRA_FILE" ]; then
    FILE_PATH=$(echo "$EXTRA_FILE" | cut -d: -f1)
    FILE_CONTENT=$(echo "$EXTRA_FILE" | cut -d: -f2)
    echo "$FILE_CONTENT" > "custom_ramdisk/$FILE_PATH"
fi

echo "[REMOTE] Creating initramfs..."
cd custom_ramdisk
find . | cpio -o -H newc > ../initramfs.cpio
cd ..

BLK_DEV=$3
sudo parted /dev/$BLK_DEV mktable gpt -a optimal --script || true
sudo parted /dev/$BLK_DEV mkpart ESP fat32 1MiB 100% --script || true
sudo parted /dev/$BLK_DEV set 1 esp on || true
sudo partprobe /dev/$BLK_DEV || true
sleep 2
sudo mkfs.fat -F32 /dev/${BLK_DEV}p1 > /dev/null
sudo mkdir -p /mnt/efi
sudo mount /dev/${BLK_DEV}p1 /mnt/efi
sudo mkdir -p /mnt/efi/EFI/BOOT
sudo cp Image /mnt/efi/EFI/BOOT/BOOTAA64.EFI
sudo cp initramfs.cpio /mnt/efi/initramfs.cpio
sudo umount /mnt/efi
sync
echo "[REMOTE] Disk write complete!"
REMOTE_EOF
}

# ── Helper: snapshot → AMI → launch ──────────────────────────────────────────
bake_and_launch() {
    local VOL_ID=$1
    local LABEL=$2
    local LABEL_LOWER=$(echo "$LABEL" | tr '[:upper:]' '[:lower:]')

    echo "── Cleaning up old $LABEL AMIs and Snapshots to save costs..." >&2
    OLD_AMIS=$(aws ec2 describe-images --region "$REGION" --owners self --filters "Name=name,Values=vein-${LABEL_LOWER}-*" --query "Images[*].[ImageId,BlockDeviceMappings[0].Ebs.SnapshotId]" --output text)
    if [ -n "$OLD_AMIS" ]; then
        while read -r OLD_AMI OLD_SNAP; do
            if [ -n "$OLD_AMI" ] && [ "$OLD_AMI" != "None" ]; then
                aws ec2 deregister-image --region "$REGION" --image-id "$OLD_AMI" >/dev/null 2>&1 || true
            fi
            if [ -n "$OLD_SNAP" ] && [ "$OLD_SNAP" != "None" ]; then
                aws ec2 delete-snapshot --region "$REGION" --snapshot-id "$OLD_SNAP" >/dev/null 2>&1 || true
            fi
        done <<< "$OLD_AMIS"
    fi

    echo "── Snapshotting $LABEL volume..." >&2
    SNAP_ID=$(aws ec2 create-snapshot --region "$REGION" \
        --volume-id "$VOL_ID" --description "Vein $LABEL" \
        --query "SnapshotId" --output text)
    aws ec2 wait snapshot-completed --region "$REGION" --snapshot-ids "$SNAP_ID"

    AMI_ID=$(aws ec2 register-image --region "$REGION" \
        --name "vein-${LABEL_LOWER}-$(date +%s)" \
        --architecture arm64 \
        --boot-mode uefi \
        --root-device-name /dev/xvda \
        --virtualization-type hvm \
        --ena-support \
        --block-device-mappings "[{\"DeviceName\": \"/dev/xvda\", \"Ebs\": {\"SnapshotId\": \"$SNAP_ID\", \"VolumeSize\": 1, \"VolumeType\": \"gp3\", \"DeleteOnTermination\": true}}]" \
        --query "ImageId" --output text)

    aws ec2 wait image-available --region "$REGION" --image-ids "$AMI_ID"

    IID=$(aws ec2 run-instances --region "$REGION" \
        --image-id "$AMI_ID" --instance-type t4g.micro --key-name "$KEY_NAME" \
        --security-group-ids sg-0dae7cfaca63e7d62 \
        --subnet-id subnet-00d743a014c24b7bb \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=Vein-$LABEL}]" \
        --query "Instances[0].InstanceId" --output text)

    echo "$IID"
}

# ═══════════════════════════════════════════════════════════
echo "  STEP 1: Build and deploy RECEIVER"
echo "═══════════════════════════════════════════════════════════"
push_workspace
RECV_VOL=$(get_or_create_vol "vein-receiver-vol")
remote_build_and_write "receiver" "" "nvme1n1"
RECV_IID=$(bake_and_launch "$RECV_VOL" "Receiver")
echo "[+] Receiver instance: $RECV_IID"

echo "[+] Waiting for Receiver to get a public IP..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$RECV_IID"
RECV_PUBLIC_IP=$(aws ec2 describe-instances --region "$REGION" \
    --instance-ids "$RECV_IID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
echo "[+] Receiver public IP: $RECV_PUBLIC_IP"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              VEIN-OS RECEIVER DEPLOYED                   ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Receiver : $RECV_IID ($RECV_PUBLIC_IP)  ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ═══════════════════════════════════════════════════════════
echo "  STEP 2: Build and deploy SENDER"
echo "═══════════════════════════════════════════════════════════"

# Get the Receiver's private VPC IP for intra-VPC comms (no public internet)
RECV_PRIVATE_IP=$(aws ec2 describe-instances --region "$REGION" \
    --instance-ids "$RECV_IID" \
    --query "Reservations[0].Instances[0].PrivateIpAddress" --output text)
echo "[+] Receiver private IP (VPC): $RECV_PRIVATE_IP"

SEND_VOL=$(get_or_create_vol "vein-sender-vol")
# Inject receiver_ip://<private_ip> into the sender's initramfs
remote_build_and_write "sender" "receiver_ip:$RECV_PRIVATE_IP" "nvme2n1"
SEND_IID=$(bake_and_launch "$SEND_VOL" "Sender")
echo "[+] Sender instance: $SEND_IID"

echo "[+] Waiting for Sender to get a public IP..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$SEND_IID"
SEND_PUBLIC_IP=$(aws ec2 describe-instances --region "$REGION" \
    --instance-ids "$SEND_IID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
echo "[+] Sender public IP: $SEND_PUBLIC_IP"

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║            VEIN-OS PAIR FULLY DEPLOYED                   ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Receiver : $RECV_IID ($RECV_PUBLIC_IP)  ║"
echo "║  Sender   : $SEND_IID ($SEND_PUBLIC_IP)  ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "Watch Receiver console (metrics will appear here):"
echo "  watch -n5 'aws ec2 get-console-output --region $REGION --instance-id $RECV_IID --output text'"
echo ""
echo "Watch Sender console:"
echo "  watch -n5 'aws ec2 get-console-output --region $REGION --instance-id $SEND_IID --output text'"

