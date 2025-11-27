#!/bin/bash

echo "=== Bắt đầu Reset Kubernetes Cluster ==="

# 1. Reset kubeadm
echo "[1/6] Running kubeadm reset..."
sudo kubeadm reset -f

# 2. Xóa thư mục cấu hình Kubernetes
echo "[2/6] Deleting /etc/kubernetes..."
sudo rm -rf /etc/kubernetes

# 3. Xóa dữ liệu etcd
echo "[3/6] Deleting /var/lib/etcd..."
sudo rm -rf /var/lib/etcd

# 4. Xóa dữ liệu kubelet
echo "[4/6] Deleting /var/lib/kubelet..."
sudo rm -rf /var/lib/kubelet/*

# 5. Xóa cấu hình mạng CNI
echo "[5/6] Deleting /etc/cni/net.d..."
sudo rm -rf /etc/cni/net.d

# 6. Xóa config file của user hiện tại
echo "[6/6] Deleting $HOME/.kube..."
sudo rm -rf $HOME/.kube

# Bước bổ sung: Xóa các rules mạng rác còn sót lại (Rất quan trọng)
echo "=== Dọn dẹp iptables ==="
sudo iptables -F && sudo iptables -t nat -F && sudo iptables -t mangle -F && sudo iptables -X

echo "=== ĐÃ HOÀN TẤT RESET! ==="
