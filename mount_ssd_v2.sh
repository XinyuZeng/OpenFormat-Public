#./mount_ssd_v2.sh vde /mnt1960iops
if [ $# -ne 2 ]
then
echo "error"
exit
fi
apt install -y parted e2fsprogs
# partition device and make GPT for it.
echo "mklabel gpt
mkpart primary 1 100%
align-check optimal 1
print
quit" | parted /dev/$1
partprobe
fdisk -lu /dev/$1
# make ext4 file system for device.
mkfs -t ext4 /dev/$1
# mount device.
cp /etc/fstab /etc/fstab.bak
echo `blkid /dev/$1 | awk '{print $2}' | sed 's/\"//g'` $2 ext4 defaults 0 0 >> /etc/fstab
cat /etc/fstab
mkdir $2
mount -a
df -h 