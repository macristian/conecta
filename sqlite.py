import os
import csv
import time
import sys
import signal
import sqlite3

# Função para interromper a execução do programa ao pressionar as teclas "CTRL + x"
def signal_handler(sig, frame):
    print('\n\nA conversão de arquivos foi interrompida pelo usuário. Total de arquivos convertidos: %d' % count)
    save_log_file()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Diretório raiz onde os arquivos CSV foram salvos
csv_directory = 'IPIC/'

# Configurações do banco de dados SQLite
db_path = os.path.join(os.getcwd(), 'ipic.db')
db_conn = sqlite3.connect(db_path)
db_cursor = db_conn.cursor()

# Cria a tabela no banco de dados se não existir
db_cursor.execute("""
    CREATE TABLE IF NOT EXISTS arquivo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        texto TEXT,
        caminho VARCHAR(255)
    )
""")

# Define o tempo de início da conversão
start_time = time.time()

# Inicializa o contador de arquivos convertidos
count = 0

# Inicializa a porcentagem de arquivos convertidos
percent_done = 0

# Lista para armazenar os arquivos que falharam ou foram pulados
failed_files = []

# Conta a quantidade total de arquivos CSV no diretório
total_files = sum(1 for root, dirs, files in os.walk(csv_directory) for filename in files if filename.lower().endswith('.csv'))

# Finaliza a barra de progresso
sys.stdout.write('\n')
print('Bem-vindo ao conversor de CSV para SQL by Cristian Bernardo \u00A9 2023\n\nAcesse: cristianbernardo.com.br\n\nPressione "CTRL + C" para interromper a conversão\n')

# Percorre recursivamente o diretório e seus subdiretórios
for root, dirs, files in os.walk(csv_directory):
    for filename in files:
        if filename.lower().endswith('.csv'):
            # Caminho completo do arquivo CSV
            csv_path = os.path.join(root, filename)

            # Abre o arquivo CSV dentro do bloco try-except
            try:
                with open(csv_path, 'r') as csv_file:
                    # Cria um objeto csv_reader
                    csv_reader = csv.reader(csv_file)

                    # Lê as linhas do arquivo CSV
                    for row in csv_reader:
                        text = row[0]  # Assume que o texto está na primeira coluna

                        # Insere os dados no banco de dados
                        insert_query = "INSERT INTO arquivo (texto, caminho) VALUES (?, ?)"
                        db_cursor.execute(insert_query, (text, csv_path))
                        db_conn.commit()

                # Incrementa o contador de arquivos convertidos
                count += 1

                # Mostra o contador de arquivos convertidos
                sys.stdout.write('\r')
                sys.stdout.write('\rArquivos convertidos: %d. Previsão de conclusão: %s' % (count, time.strftime('%H:%M:%S', time.gmtime((time.time() - start_time) / count * (total_files - count)))))

                # Incrementa a porcentagem de arquivos convertidos
                percent_done += 1 / total_files

                # Mostra a barra de progresso
                progress_bar = "[%-20s] %.2f%% %s %s" % ('='*int(percent_done*20), percent_done*100, time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time)), filename)
                sys.stdout.write(progress_bar[:80])
                if len(progress_bar) > 80:
                    sys.stdout.write('...')
                sys.stdout.flush()

                # Imprime a animação de "convertendo"
                sys.stdout.write(' Convertendo ')
                animation = "|/-\\"
                for i in range(20):
                    time.sleep(0.1)
                    sys.stdout.write(animation[i % len(animation)])
                    sys.stdout.flush()
                    sys.stdout.write('\b')

            except Exception as e:
                print(f"Erro ao ler o arquivo CSV: {e}")
                failed_files.append((csv_path, 'Erro ao ler o arquivo CSV'))

# Finaliza a barra de progresso, o contador de arquivos convertidos e a animação
sys.stdout.write('\r')
sys.stdout.write(' ' * 80)
sys.stdout.write('\r')
print('Conversão finalizada. Total de arquivos convertidos: %d' % count)

# Função para salvar o arquivo de log
def save_log_file():
    log_path = os.path.join(os.getcwd(), 'log-4.txt')
    with open(log_path, 'w') as log_file:
        log_file.write('Data de execução: ' + time.strftime('%Y-%m-%d %H:%M:%S') + '\n')
        log_file.write('Quantidade de arquivos convertidos: ' + str(count) + '\n\n')
        log_file.write('Arquivos que falharam ou foram pulados:\n')
        for failed_file in failed_files:
            log_file.write(failed_file[0] + ' - ' + failed_file[1] + '\n')

# Salva o arquivo de log
save_log_file()

# Fecha a conexão com o banco de dados
db_cursor.close()
db_conn.close()