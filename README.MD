 

# mdtest
precisa o gcc
apt install gcc

go mod tidy

go mod init tabelaIbpt


#baixar todas todas dependencias
go get -d ./...          

 
1. Clonar repositório.
2. Executar `go build` dentro deste diretório.
       # `env GOOS=linux GOARCH=amd64 go build `-> caso der erro de architetura arm amd64 etc...
3. Executar `./tabelaIbpt` para iniciar o bot.
    Adicione `-debug` se você quiser ver os dados sendo enviados/recebidos.
4. Na primeira execução, escaneie o código QR. Em execuções futuras, o programa lembrará de você (a menos que `tabelaIbpt-logs.db` seja excluído).
5. para ver os logs e ler o qrcode rodar
    journalctl -xefu ibpt
ou
journalctl -f -n 100  -u ibpt
 
curl -s "http://localhost:8090/api/v1/produtos?token=TWFycmVjb01hcmluZ2FQcmVzbzIwMjE&codigo=71171900&uf=SC&valor=100"

git pull && env GOOS=linux GOARCH=amd64 go build &&  service tabelaIbpt restart && journalctl -f -n 100  -u tabelaIbpt
6. para matar o processo:
    ps afux |grep codigos | awk '{print "kill -9", $2}' |sh
7. PRA ATUALIZAR O LIB
    go get -u go.mau.fi/whatsmeow

CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build         

NA AMAZON INICIAR O SERVICO ASSIM: 
# systemctl start tabelaIbpt

Jan 13 12:16:15 ip-172-31-34-249 tabelaIbpt[61614]: goroutine 788 [running]:
Jan 13 12:16:15 ip-172-31-34-249 tabelaIbpt[61614]: net/http.(*conn).serve.func1()
Jan 13 12:16:15 ip-172-31-34-249 tabelaIbpt[61614]:         /usr/local/go/src/net/http/server.go:1868 +0xb9



https://fabianlee.org/2022/10/29/golang-running-a-go-binary-as-a-systemd-service-on-ubuntu-22-04/



vi /lib/systemd/system/ibpt.service 


****************************************************************
[Unit]
Description=ibpt

[Service]
Type=simple
Restart=always
RestartSec=30s
WorkingDirectory=/home/apps/go/tabelaIbpt
ExecStart=/home/apps/go/tabelaIbpt/tabelaIbpt

[Install]
WantedBy=multi-user.target
****************************************************************


systemctl daemon-reload


ativa o servico
 systemctl enable ibpt


 
 systemctl start ibpt.service
journalctl -f -n 100  -u ibpt