git pull && go build && systemctl restart ibpt && journalctl -f -n 100  -u ibpt
