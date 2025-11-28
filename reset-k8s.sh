echo "=== Bắt đầu Reset Kubernetes Cluster ==="

echo "[1/6] Running kubeadm reset..."
sudo kubeadm reset -f

echo "[2/6] Deleting /etc/kubernetes..."
sudo rm -rf /etc/kubernetes

echo "[3/6] Deleting /var/lib/etcd..."
sudo rm -rf /var/lib/etcd

echo "[4/6] Deleting /var/lib/kubelet..."
sudo rm -rf /var/lib/kubelet/*

echo "[5/6] Deleting /etc/cni/net.d..."
sudo rm -rf /etc/cni/net.d

echo "[6/6] Deleting $HOME/.kube..."
sudo rm -rf $HOME/.kube

echo "=== ĐÃ HOÀN TẤT RESET! ==="
