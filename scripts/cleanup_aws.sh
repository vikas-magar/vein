#!/bin/bash
REGION="ap-south-2"

echo "=== Starting AWS Cleanup in $REGION ==="

echo "1. Deregistering all custom AMIs..."
AMI_IDS=$(aws ec2 describe-images --region "$REGION" --owners self --query "Images[*].ImageId" --output text)
for AMI in $AMI_IDS; do
    echo "  Deregistering $AMI..."
    aws ec2 deregister-image --region "$REGION" --image-id "$AMI"
done

echo "2. Deleting all snapshots..."
# AMIs hold snapshots. Once AMIs are deregistered, we can delete the snapshots.
# We might need to wait a few seconds.
sleep 5
SNAP_IDS=$(aws ec2 describe-snapshots --region "$REGION" --owner-ids self --query "Snapshots[*].SnapshotId" --output text)
for SNAP in $SNAP_IDS; do
    echo "  Deleting $SNAP..."
    aws ec2 delete-snapshot --region "$REGION" --snapshot-id "$SNAP" || true
done

echo "3. Deleting all unattached (available) volumes..."
VOL_IDS=$(aws ec2 describe-volumes --region "$REGION" --filters "Name=status,Values=available" --query "Volumes[*].VolumeId" --output text)
for VOL in $VOL_IDS; do
    echo "  Deleting $VOL..."
    aws ec2 delete-volume --region "$REGION" --volume-id "$VOL" || true
done

echo "=== Cleanup Complete! ==="
