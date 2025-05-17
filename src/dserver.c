#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include "../include/common.h"


char document_folder[MAX_PATH_SIZE];
int cache_size;
Document *documents = NULL;  // Array de documentos
int next_id = 1;             // Próximo ID disponível
int num_documents = 0;       // Número atual de documentos

// Declarações de funções - adicionadas para resolver os erros de compilação
int search_documents_sequential(const char *keyword, int *doc_ids, int max_results);
int search_for_keyword(const char *filepath, const char *keyword);
int count_keyword_lines(const char *filepath, const char *keyword);
int search_documents(const char *keyword, int *doc_ids, int max_results, int nr_processes);

// Função para verificar se um arquivo contém uma palavra-chave
int search_for_keyword(const char *filepath, const char *keyword) {
    int fd = open(filepath, O_RDONLY);
    if (fd == -1) {
        perror("Erro ao abrir arquivo para busca");
        return 0; // Arquivo não existe ou erro
    }
    
    char buffer[4096];
    int bytes_read;
    int result = 0;
    
    while ((bytes_read = read(fd, buffer, sizeof(buffer) - 1)) > 0) {
        // Garantir que o buffer termine com \0 para usar strstr
        buffer[bytes_read] = '\0';
        
        if (strstr(buffer, keyword) != NULL) {
            result = 1;
            break;
        }
    }
    
    close(fd);
    return result;
}

// Função de pesquisa sequencial 
int search_documents_sequential(const char *keyword, int *doc_ids, int max_results) {
    int count = 0;
    
    for (int i = 0; i < num_documents && count < max_results; i++) {
        // Construir caminho completo
        char full_path[MAX_PATH_SIZE * 2];
        sprintf(full_path, "%s/%s", document_folder, documents[i].path);
        
        // Usar a nossa própria função de busca
        if (search_for_keyword(full_path, keyword)) {
            // Palavra-chave encontrada
            doc_ids[count++] = documents[i].id;
        }
    }
    
    return count;
}

// Função para inicializar o servidor
int initialize_server() {
    // Remover pipe do servidor se já existir
    unlink(SERVER_PIPE);
    
    // Criar pipe do servidor
    if (mkfifo(SERVER_PIPE, 0666) == -1) {
        perror("Erro ao criar pipe do servidor");
        return -1;
    }
    
    // Alocar memória para documentos
    documents = (Document*)malloc(sizeof(Document) * cache_size);
    if (!documents) {
        perror("Erro ao alocar memória");
        return -1;
    }
    
    printf("Servidor iniciado. Aguardar conexões...\n");
    return 0;
}

// Limpar recursos ao encerrar
void cleanup() {
    if (documents != NULL) {
        free(documents);
        documents = NULL;  // Importante definir como NULL após liberar
    }
    unlink(SERVER_PIPE);
    printf("Servidor encerrado.\n");
}

// Adicionar um documento
int add_document(ClientMessage *msg) {
    if (num_documents >= cache_size) {
        return -1; // Cache cheio
    }
    
    // Verificar se o documento existe
    char full_path[MAX_PATH_SIZE * 2];
    sprintf(full_path, "%s/%s", document_folder, msg->path);
    
    // Verificar se o arquivo existe
    if (access(full_path, F_OK) == -1) {
        return -2; // Arquivo não existe
    }
    
    // Adicionar documento
    Document doc;
    doc.id = next_id++;
    strncpy(doc.title, msg->title, MAX_TITLE_SIZE - 1);
    doc.title[MAX_TITLE_SIZE - 1] = '\0';  // Garantir encerramento
    strncpy(doc.authors, msg->authors, MAX_AUTHORS_SIZE - 1);
    doc.authors[MAX_AUTHORS_SIZE - 1] = '\0';
    strncpy(doc.year, msg->year, MAX_YEAR_SIZE - 1);
    doc.year[MAX_YEAR_SIZE - 1] = '\0';
    strncpy(doc.path, msg->path, MAX_PATH_SIZE - 1);
    doc.path[MAX_PATH_SIZE - 1] = '\0';
    
    documents[num_documents++] = doc;
    
    return doc.id;
}

// Consultar um documento
int consult_document(int doc_id, Document *doc) {
    for (int i = 0; i < num_documents; i++) {
        if (documents[i].id == doc_id) {
            *doc = documents[i];
            return 0;
        }
    }
    return -1; // Documento não encontrado
}

// Remover um documento
int delete_document(int doc_id) {
    for (int i = 0; i < num_documents; i++) {
        if (documents[i].id == doc_id) {
            // Mover o último documento para a posição do documento removido
            if (i < num_documents - 1) {
                documents[i] = documents[num_documents - 1];
            }
            num_documents--;
            return 0;
        }
    }
    return -1; // Documento não encontrado
}

// Contar linhas com uma palavra-chave
int count_lines(int doc_id, const char *keyword) {
    Document doc;
    if (consult_document(doc_id, &doc) != 0) {
        return -1; // Documento não encontrado
    }
    
    // Construir caminho completo
    char full_path[MAX_PATH_SIZE * 2];
    sprintf(full_path, "%s/%s", document_folder, doc.path);
    
    // Construir comando grep
    char command[512];
    sprintf(command, "grep -c \"%s\" \"%s\"", keyword, full_path);
    
    // Executar comando
    FILE *fp = popen(command, "r");
    if (fp == NULL) {
        return -2; // Erro ao executar comando
    }
    
    // Ler resultado
    char buffer[32];
    fgets(buffer, sizeof(buffer), fp);
    pclose(fp);
    
    return atoi(buffer);
}


int search_documents(const char *keyword, int *doc_ids, int max_results, int nr_processes) {
    // Se nr_processes for 1 ou menos, usar método sequencial
    if (nr_processes <= 1) {
        return search_documents_sequential(keyword, doc_ids, max_results);
    }
    
    int count = 0;
    int pipes[nr_processes][2];
    pid_t pids[nr_processes];
    
    // Limitar número de processos ao número de documentos
    if (nr_processes > num_documents) {
        nr_processes = num_documents;
    }
    
    // Criar pipes para comunicação
    for (int i = 0; i < nr_processes; i++) {
        if (pipe(pipes[i]) == -1) {
            perror("Erro ao criar pipe");
            // Se falhar em criar pipes, usar método sequencial
            return search_documents_sequential(keyword, doc_ids, max_results);
        }
    }
    
    // Dividir documentos entre processos
    int docs_per_process = (num_documents + nr_processes - 1) / nr_processes;
    
    // Criar processos filhos
    for (int i = 0; i < nr_processes; i++) {
        pids[i] = fork();
        
        if (pids[i] == -1) {
            perror("Erro ao criar processo");
            // Se falhar em criar processo, usar apenas os já criados
            nr_processes = i;
            if (nr_processes == 0) {
                return search_documents_sequential(keyword, doc_ids, max_results);
            }
            break;
        }
        
        if (pids[i] == 0) {
            // Código do processo filho
            
          
            for (int j = 0; j < nr_processes; j++) {
                if (j != i) {
                    close(pipes[j][0]);
                    close(pipes[j][1]);
                }
            }
            
            close(pipes[i][0]); 
            
            int start = i * docs_per_process;
            int end = (i + 1) * docs_per_process;
            if (end > num_documents) end = num_documents;
            
            int child_count = 0;
            int child_results[end - start];
            
            // Pesquisar documentos alocados a este processo
            for (int j = start; j < end; j++) {
                char full_path[MAX_PATH_SIZE * 2];
                sprintf(full_path, "%s/%s", document_folder, documents[j].path);
                
                if (search_for_keyword(full_path, keyword)) {
                    child_results[child_count++] = documents[j].id;
                }
            }
            
            // Enviar resultados para o processo pai
            if (write(pipes[i][1], &child_count, sizeof(int)) < 0) {
                perror("Erro ao escrever contagem no pipe");
            }
            
            if (child_count > 0) {
                if (write(pipes[i][1], child_results, sizeof(int) * child_count) < 0) {
                    perror("Erro ao escrever resultados no pipe");
                }
            }
            
            close(pipes[i][1]);
            _exit(0); 
        } else {
            // Processo pai
            close(pipes[i][1]); 
        }
    }
    
    // Coletar resultados dos processos filhos
    for (int i = 0; i < nr_processes; i++) {
        int child_count = 0;
        int read_result = read(pipes[i][0], &child_count, sizeof(int));
        
        if (read_result <= 0) {
            // Erro na leitura ou pipe fechado
            fprintf(stderr, "Aviso: Falha ao ler do processo %d\n", i);
            close(pipes[i][0]);
            continue;
        }
        
        if (child_count > 0) {
            int child_results[child_count];
            if (read(pipes[i][0], child_results, sizeof(int) * child_count) <= 0) {
                fprintf(stderr, "Aviso: Falha ao ler resultados do processo %d\n", i);
            } else {
                // Adicionar resultados ao array final
                for (int j = 0; j < child_count && count < max_results; j++) {
                    doc_ids[count++] = child_results[j];
                }
            }
        }
        
        close(pipes[i][0]);
        
        // Esperar pelo encerramento do processo filho com timeout
        int status;
        pid_t wait_result = waitpid(pids[i], &status, WNOHANG);
        
        if (wait_result == 0) {
            // Processo ainda em execução, dar um tempo adicional
            usleep(100000); // 100ms
            wait_result = waitpid(pids[i], &status, WNOHANG);
            
            if (wait_result == 0) {
                // Se ainda estiver a executar, forçar encerramento
                fprintf(stderr, "Aviso: Processo %d não terminou, enviando SIGTERM\n", i);
                kill(pids[i], SIGTERM);
                usleep(50000); // 50ms
                waitpid(pids[i], NULL, WNOHANG);
            }
        }
    }
    
    // Garantir que todos os processos filhos foram encerrados
    for (int i = 0; i < nr_processes; i++) {
        kill(pids[i], SIGTERM); 
        waitpid(pids[i], NULL, WNOHANG);
    }
    
    return count;
}


int main(int argc, char *argv[]) {
    // Verificar argumentos
    if (argc != 3) {
        fprintf(stderr, "Uso: %s document_folder cache_size\n", argv[0]);
        return 1;
    }
    
    // Obter argumentos
    strncpy(document_folder, argv[1], MAX_PATH_SIZE - 1);
    document_folder[MAX_PATH_SIZE - 1] = '\0';  // Garantir encerramento
    
    cache_size = atoi(argv[2]);
    if (cache_size <= 0) {
        fprintf(stderr, "Tamanho de cache inválido. Deve ser maior que zero.\n");
        return 1;
    }
    
    printf("Pasta de documentos: %s\n", document_folder);
    printf("Tamanho do cache: %d\n", cache_size);
    
    // Inicializar servidor
    if (initialize_server() < 0) {
        return 1;
    }
    
    // Configurar limpeza ao encerrar
    atexit(cleanup);
    
    // Abrir pipe para leitura
    int server_pipe = open(SERVER_PIPE, O_RDONLY);
    if (server_pipe == -1) {
        perror("Erro ao abrir pipe do servidor");
        return 1;
    }
    
    printf("Aguardar conexões de clientes...\n");
    
    // Loop principal do servidor
    ClientMessage client_msg;
    ServerMessage server_response;
    char client_pipe_name[100];
    int client_pipe;

    while(1) {
        // Ler mensagem do cliente
        ssize_t bytes_read = read(server_pipe, &client_msg, sizeof(ClientMessage));
        
        if (bytes_read > 0) {
            printf("Mensagem recebida do cliente PID %d, operação %d\n", 
                client_msg.pid, client_msg.operation);
            
            // Configurar nome do pipe do cliente
            sprintf(client_pipe_name, "%s%d", CLIENT_PIPE_PREFIX, client_msg.pid);
            
            // Inicializar resposta
            memset(&server_response, 0, sizeof(ServerMessage));
            
            // Processar mensagem de acordo com a operação
            switch(client_msg.operation) {
                case OP_ADD:
                    printf("A Adicionar documento: %s\n", client_msg.title);
                    server_response.doc_id = add_document(&client_msg);
                    
                    if (server_response.doc_id > 0) {
                        server_response.status = 0;
                    } else if (server_response.doc_id == -1) {
                        server_response.status = -1;
                        strcpy(server_response.error_msg, "Cache cheio");
                    } else {
                        server_response.status = -1;
                        strcpy(server_response.error_msg, "Arquivo não encontrado");
                    }
                    break;
                    
                case OP_CONSULT:
                    printf("Consultar documento: %d\n", client_msg.doc_id);
                    if (consult_document(client_msg.doc_id, &server_response.doc) == 0) {
                        server_response.status = 0;
                    } else {
                        server_response.status = -1;
                        strcpy(server_response.error_msg, "Documento não encontrado");
                    }
                    break;
                    
                case OP_DELETE:
                    printf("Remover documento: %d\n", client_msg.doc_id);
                    if (delete_document(client_msg.doc_id) == 0) {
                        server_response.status = 0;
                    } else {
                        server_response.status = -1;
                        strcpy(server_response.error_msg, "Documento não encontrado");
                    }
                    break;
                    
                case OP_LINES:
                    printf("Contar linhas no documento %d com palavra-chave: %s\n", 
                           client_msg.doc_id, client_msg.keyword);
                    server_response.line_count = count_lines(client_msg.doc_id, client_msg.keyword);
                    
                    if (server_response.line_count >= 0) {
                        server_response.status = 0;
                    } else if (server_response.line_count == -1) {
                        server_response.status = -1;
                        strcpy(server_response.error_msg, "Documento não encontrado");
                    } else {
                        server_response.status = -1;
                        strcpy(server_response.error_msg, "Erro ao contar linhas");
                    }
                    break;
                    
                case OP_SEARCH:
                    printf("Pesquisar documentos com palavra-chave: %s (processos: %d)\n", 
                           client_msg.keyword, client_msg.nr_processes);
                   
                    server_response.doc_count = search_documents(client_msg.keyword, 
                                                                server_response.doc_ids, 
                                                                1024,
                                                                client_msg.nr_processes);
                    server_response.status = 0;
                    break;
                    
                case OP_SHUTDOWN:
                    printf("Comando de desligamento recebido\n");
                    server_response.status = 0;
                    
                    // Abrir pipe do cliente para enviar confirmação
                    client_pipe = open(client_pipe_name, O_WRONLY);
                    if (client_pipe != -1) {
                        write(client_pipe, &server_response, sizeof(ServerMessage));
                        close(client_pipe);
                    }
                    
                    // Encerrar o servidor
                    close(server_pipe);
                    
                    exit(0);
                    break;
                    
                default:
                    printf("Operação não reconhecida\n");
                    server_response.status = -1;
                    strcpy(server_response.error_msg, "Operação não reconhecida");
                    break;
            }
            
            // Enviar resposta (exceto para OP_SHUTDOWN, que já enviou)
            if (client_msg.operation != OP_SHUTDOWN) {
                client_pipe = open(client_pipe_name, O_WRONLY);
                if (client_pipe != -1) {
                    write(client_pipe, &server_response, sizeof(ServerMessage));
                    close(client_pipe);
                } else {
                    perror("Erro ao abrir pipe do cliente");
                }
            }
        }
    }
    
    close(server_pipe);
    
    return 0;
}