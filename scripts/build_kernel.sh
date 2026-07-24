#!/bin/bash
set -e

REGION="ap-south-2"
KEY_NAME="hermit-key"
KEY_FILE="../config/hermit-key.pem"
PROJECT_DIR=$(pwd)

echo "═══════════════════════════════════════════════════════════"
echo "  1. Provisioning Kernel Builder Instance..."
echo "═══════════════════════════════════════════════════════════"
INSTANCE_ID=$(aws ec2 describe-instances --region "$REGION" \
    --filters "Name=tag:Name,Values=kernel-builder" "Name=instance-state-name,Values=running" \
    --query "Reservations[0].Instances[0].InstanceId" --output text)

if [ "$INSTANCE_ID" == "None" ] || [ -z "$INSTANCE_ID" ]; then
    echo "[+] Launching new Spot Builder (t4g.small for Kernel builds)..."
    UBUNTU_AMI=$(aws ec2 describe-images --region "$REGION" --owners 099720109477 \
        --filters "Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-arm64-server-*" "Name=state,Values=available" \
        --query "sort_by(Images, &CreationDate)[-1].ImageId" --output text)
        
    SG_ID=$(aws ec2 describe-security-groups --region "$REGION" \
        --filters "Name=group-name,Values=hermit-builder-sg" \
        --query "SecurityGroups[0].GroupId" --output text)

    INSTANCE_ID=$(aws ec2 run-instances --region "$REGION" \
        --image-id "$UBUNTU_AMI" --instance-type t4g.small --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=60,VolumeType=gp3}' \
        --instance-market-options '{"MarketType":"spot"}' \
        --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=kernel-builder}]' \
        --query "Instances[0].InstanceId" --output text)
        
    aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"
fi

PUBLIC_IP=$(aws ec2 describe-instances --region "$REGION" --instance-ids "$INSTANCE_ID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" --output text)

echo "[+] Builder IP: $PUBLIC_IP"
while ! ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i "$KEY_FILE" ubuntu@"$PUBLIC_IP" 'echo ok' &>/dev/null; do
    sleep 2
done

echo "═══════════════════════════════════════════════════════════"
echo "  2. Syncing Configurations..."
echo "═══════════════════════════════════════════════════════════"
# We only sync the linux config and init.c
rsync -az -e "ssh -o StrictHostKeyChecking=no -i $KEY_FILE" \
    ../config/microvm.config ubuntu@$PUBLIC_IP:/home/ubuntu/
rsync -az -e "ssh -o StrictHostKeyChecking=no -i $KEY_FILE" \
    ../src/init.c ubuntu@$PUBLIC_IP:/home/ubuntu/

echo "═══════════════════════════════════════════════════════════"
echo "  3. Compiling Linux Kernel on Graviton..."
echo "═══════════════════════════════════════════════════════════"
ssh -o StrictHostKeyChecking=no -i "$KEY_FILE" ubuntu@"$PUBLIC_IP" 'bash -s' << 'REMOTE_EOF'
set -e
sudo apt-get update -q && sudo apt-get install -q -y build-essential wget xz-utils libssl-dev pkg-config flex bison bc libelf-dev dwarves libclang-dev clang llvm lld

echo "[REMOTE] Configuring 8GB Swap Space to prevent OOM..."
if [ ! -f /swapfile ]; then
    sudo fallocate -l 8G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
fi

# Compile the C wrapper (One-time compile for arm64)
gcc -static init.c -o /home/ubuntu/init
chmod +x /home/ubuntu/init

if [ ! -d "linux-7.2-rc2" ]; then
    echo "[REMOTE] Downloading Linux (first run only)..."
    wget -q https://git.kernel.org/torvalds/t/linux-7.2-rc2.tar.gz
    tar -xf linux-7.2-rc2.tar.gz
fi
cd linux-7.2-rc2

cp ../microvm.config .

echo "[REMOTE] Generating config..."
wget -q -O .config https://raw.githubusercontent.com/firecracker-microvm/firecracker/main/resources/guest_configs/microvm-kernel-ci-aarch64-6.1.config
scripts/kconfig/merge_config.sh -m .config microvm.config > /dev/null
make ARCH=arm64 olddefconfig > /dev/null

echo "[REMOTE] Compiling kernel (Image)..."
make ARCH=arm64 -j$(nproc) Image

# Move the final Image to home directory
cp arch/arm64/boot/Image /home/ubuntu/Image
REMOTE_EOF

echo "═══════════════════════════════════════════════════════════"
echo "  4. Downloading Final Linux Artifacts..."
echo "═══════════════════════════════════════════════════════════"
scp -o StrictHostKeyChecking=no -i "$KEY_FILE" ubuntu@$PUBLIC_IP:/home/ubuntu/Image ../linux/Image
scp -o StrictHostKeyChecking=no -i "$KEY_FILE" ubuntu@$PUBLIC_IP:/home/ubuntu/init ../linux/init
echo "[+] Successfully built and downloaded the Linux Kernel Image and C Init wrapper to ../linux/!"
