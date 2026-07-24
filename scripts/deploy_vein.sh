#!/bin/bash
set -e

REGION="ap-south-2"
KEY_NAME="hermit-key"
KEY_FILE="../config/hermit-key.pem"
PROJECT_DIR=$(pwd)

echo "═══════════════════════════════════════════════════════════"
echo "  1. Getting AMI Factory Instance..."
echo "═══════════════════════════════════════════════════════════"
# We reuse the kernel-builder instance as our AMI factory
INSTANCE_ID=$(aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:Name,Values=kernel-builder" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].InstanceId" --output text)

PUBLIC_IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" --output text)

echo "[+] Factory IP: $PUBLIC_IP"

echo "═══════════════════════════════════════════════════════════"
echo "  2. Pushing Pre-Compiled Kernel and Rust Workspace..."
echo "═══════════════════════════════════════════════════════════"
TARGET_BIN=${1:-"welcome"}
echo "[+] Deploying binary: $TARGET_BIN"

ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" ubuntu@"$PUBLIC_IP" 'mkdir -p /home/ubuntu/vein_workspace'
rsync -az --exclude='target/' -e "ssh -o StrictHostKeyChecking=no -i $KEY_FILE" \
    ../crates ../Cargo.toml ubuntu@$PUBLIC_IP:/home/ubuntu/vein_workspace/
# We also push the generic kernel
rsync -az -e "ssh -o StrictHostKeyChecking=no -i $KEY_FILE" \
    ../linux/Image ubuntu@$PUBLIC_IP:/home/ubuntu/

echo "═══════════════════════════════════════════════════════════"
echo "  3. Creating Deployment Disk (1GB EBS)..."
echo "═══════════════════════════════════════════════════════════"
VOL_ID=$(aws ec2 describe-volumes --region "$REGION" \
    --filters "Name=attachment.instance-id,Values=$INSTANCE_ID" "Name=tag:Name,Values=vein-deploy-vol" \
    --query "Volumes[0].VolumeId" --output text)

if [ "$VOL_ID" == "None" ] || [ -z "$VOL_ID" ]; then
    echo "[+] Creating 1GB EBS volume..."
    AVAIL_ZONE=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" --query "Reservations[0].Instances[0].Placement.AvailabilityZone" --output text)
    VOL_ID=$(aws ec2 create-volume --region "$REGION" --availability-zone "$AVAIL_ZONE" --size 1 --volume-type gp3 --tag-specifications 'ResourceType=volume,Tags=[{Key=Name,Value=vein-deploy-vol}]' --query "VolumeId" --output text)
    aws ec2 wait volume-available --region "$REGION" --volume-ids "$VOL_ID"
    aws ec2 attach-volume --region "$REGION" --volume-id "$VOL_ID" --instance-id "$INSTANCE_ID" --device /dev/sdf
    sleep 5
fi

echo "═══════════════════════════════════════════════════════════"
echo "  4. Compiling Rust & Assembling Initramfs..."
echo "═══════════════════════════════════════════════════════════"
ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" ubuntu@"$PUBLIC_IP" 'bash -s' "$TARGET_BIN" << 'REMOTE_EOF'
TARGET_BIN=$1
set -e
sudo apt-get update && sudo apt-get install -y cmake clang llvm musl-tools
export CC_aarch64_unknown_linux_musl=musl-gcc
if [ ! -f "$HOME/.cargo/env" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
    rustup target add aarch64-unknown-linux-musl
else
    source $HOME/.cargo/env
fi

if ! command -v bpf-linker &> /dev/null; then
    echo "[REMOTE] Installing bpf-linker from source (this will take a few minutes)..."
    cargo install bpf-linker
fi

echo "[REMOTE] Compiling Rust (Static Musl)..."
cd vein_workspace
cargo fetch
sed -i 's/libc::SIOCETHTOOL, \&mut ifreq/libc::SIOCETHTOOL.try_into().unwrap(), \&mut ifreq/g' $HOME/.cargo/registry/src/*/s2n-quic-xdp-0.83.0/src/syscall.rs || true
cargo build --bin $TARGET_BIN --target aarch64-unknown-linux-musl --release
cd ..

rm -rf custom_ramdisk
mkdir -p custom_ramdisk
# Use the compiled Rust binary directly as PID 1 /init
cp vein_workspace/target/aarch64-unknown-linux-musl/release/$TARGET_BIN custom_ramdisk/init
chmod +x custom_ramdisk/init

echo "[REMOTE] Zipping initramfs.cpio..."
cd custom_ramdisk
find . | cpio -o -H newc > ../initramfs.cpio
cd ..

echo "[REMOTE] Formatting and Writing EFI Disk..."
BLK_DEV=$(lsblk -dno NAME | grep nvme | grep -v 'nvme0n1' | head -n 1)
sudo parted /dev/$BLK_DEV mktable gpt -a optimal --script || true
sudo parted /dev/$BLK_DEV mkpart ESP fat32 1MiB 100% --script || true
sudo parted /dev/$BLK_DEV set 1 esp on || true
sudo partprobe /dev/$BLK_DEV || true
sleep 2
sudo mkfs.fat -F32 /dev/${BLK_DEV}p1 > /dev/null
sudo mkdir -p /mnt/efi
sudo mount /dev/${BLK_DEV}p1 /mnt/efi
sudo mkdir -p /mnt/efi/EFI/BOOT
# Copy the kernel Image directly
sudo cp Image /mnt/efi/EFI/BOOT/BOOTAA64.EFI
# Copy our zipped rust binary ramdisk to root of EFI partition
sudo cp initramfs.cpio /mnt/efi/initramfs.cpio
sudo umount /mnt/efi
echo "[REMOTE] Disk write complete!"
REMOTE_EOF

echo "═══════════════════════════════════════════════════════════"
echo "  5. Snapshot & AMI Registration"
echo "═══════════════════════════════════════════════════════════"
SNAP_ID=$(aws ec2 create-snapshot --region "$REGION" --volume-id "$VOL_ID" --description "Vein Deploy" --query "SnapshotId" --output text)
aws ec2 wait snapshot-completed --region "$REGION" --snapshot-ids "$SNAP_ID"

AMI_ID=$(aws ec2 register-image --region "$REGION" \
    --name "vein-os-$(date +%s)" \
    --architecture arm64 \
    --root-device-name /dev/xvda \
    --virtualization-type hvm \
    --ena-support \
    --block-device-mappings "[{\"DeviceName\": \"/dev/xvda\", \"Ebs\": {\"SnapshotId\": \"$SNAP_ID\", \"VolumeSize\": 1, \"VolumeType\": \"gp3\", \"DeleteOnTermination\": true}}]" \
    --query "ImageId" --output text)

echo "═══════════════════════════════════════════════════════════"
echo "  6. Launching Vein-OS (Hello World)"
echo "═══════════════════════════════════════════════════════════"
aws ec2 wait image-available --region "$REGION" --image-ids "$AMI_ID"

TEST_INSTANCE=$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI_ID" --instance-type t4g.micro --key-name "$KEY_NAME" \
    --query "Instances[0].InstanceId" --output text)

echo "✓ Validation Complete! Test Instance Launched: $TEST_INSTANCE"
