#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "common.h"


void show_usage(char *program_name) {
    fprintf(stderr, "Uso:\n");
    fprintf(stderr, "  %s -a \"title\" \"authors\" \"year\" \"path\"\n", program_name);
    fprintf(stderr, "  %s -c \"key\"\n", program_name);
    fprintf(stderr, "  %s -d \"key\"\n", program_name);
    fprintf(stderr, "  %s -l \"key\" \"keyword\"\n", program_name);
    fprintf(stderr, "  %s -s \"keyword\"\n", program_name);
    fprintf(stderr, "  %s -s \"keyword\" \"nr_processes\"\n", program_name);
    fprintf(stderr, "  %s -f\n", program_name);
}

// Cria pipe do clienteeee
int create_client_pipe(char *pipe_name) {
    unlink(pipe_name);  // Remove se já existir
    if (mkfifo(pipe_name, 0666) == -1) {
        perror("Erro ao criar pipe do cliente");
        return -1;
    }
    return 0;
}

// Envia mensagem para o servidor e recebe respostaa
int send_receive(ClientMessage *msg, ServerMessage *response, char *client_pipe) {
    // Criar pipe do cliente
    if (create_client_pipe(client_pipe) < 0) {
        return -1;
    }
    
    // Abrir pipe do servidor
    int server_pipe = open(SERVER_PIPE, O_WRONLY);
    if (server_pipe == -1) {
        perror("Erro ao abrir pipe do servidor. O servidor está em execução?");
        unlink(client_pipe);
        return -1;
    }
    
    // Enviar mensagem
    write(server_pipe, msg, sizeof(ClientMessage));
    close(server_pipe);
    
    // Aguardar resposta
    int client_fd = open(client_pipe, O_RDONLY);
    if (client_fd == -1) {
        perror("Erro ao abrir pipe do cliente");
        unlink(client_pipe);
        return -1;
    }
    
    read(client_fd, response, sizeof(ServerMessage));
    close(client_fd);
    
    // Remove pipe do cliente
    unlink(client_pipe);
    
    return 0;
}

int main(int argc, char *argv[]) {
    // Verifica se há argumentos suficientes
    if (argc < 2) {
        show_usage(argv[0]);
        return 1;
    }
    
    // Identifica operação solicitada
    char *option = argv[1];
    
    // Constroi pipe do cliente
    char client_pipe[100];
    sprintf(client_pipe, "%s%d", CLIENT_PIPE_PREFIX, getpid());
    
    // Preparar mensagem
    ClientMessage msg;
    ServerMessage response;
    memset(&msg, 0, sizeof(ClientMessage));
    msg.pid = getpid();
    
    // Verificar a opção específica
    if (strcmp(option, "-a") == 0) {
        // Adicionar documento
        if (argc != 6) {
            fprintf(stderr, "Uso incorreto do comando -a\n");
            show_usage(argv[0]);
            return 1;
        }
        
        msg.operation = OP_ADD;
        strncpy(msg.title, argv[2], MAX_TITLE_SIZE - 1);
        strncpy(msg.authors, argv[3], MAX_AUTHORS_SIZE - 1);
        strncpy(msg.year, argv[4], MAX_YEAR_SIZE - 1);
        strncpy(msg.path, argv[5], MAX_PATH_SIZE - 1);
        
        if (send_receive(&msg, &response, client_pipe) < 0) {
            return 1;
        }
        
        if (response.status == 0) {
            printf("Document %d indexed\n", response.doc_id);
        } else {
            printf("Error: %s\n", response.error_msg);
        }
    } 
    else if (strcmp(option, "-c") == 0) {
        // Consultar documento
        if (argc != 3) {
            fprintf(stderr, "Uso incorreto do comando -c\n");
            show_usage(argv[0]);
            return 1;
        }
        
        msg.operation = OP_CONSULT;
        msg.doc_id = atoi(argv[2]);
        
        if (send_receive(&msg, &response, client_pipe) < 0) {
            return 1;
        }
        
        if (response.status == 0) {
            printf("Title: %s\n", response.doc.title);
            printf("Authors: %s\n", response.doc.authors);
            printf("Year: %s\n", response.doc.year);
            printf("Path: %s\n", response.doc.path);
        } else {
            printf("Error: %s\n", response.error_msg);
        }
    }
    else if (strcmp(option, "-d") == 0) {
        // Remover documento
        if (argc != 3) {
            fprintf(stderr, "Uso incorreto do comando -d\n");
            show_usage(argv[0]);
            return 1;
        }
        
        msg.operation = OP_DELETE;
        msg.doc_id = atoi(argv[2]);
        
        if (send_receive(&msg, &response, client_pipe) < 0) {
            return 1;
        }
        
        if (response.status == 0) {
            printf("Index entry %d deleted\n", msg.doc_id);
        } else {
            printf("Error: %s\n", response.error_msg);
        }
    }
    else if (strcmp(option, "-l") == 0) {
        // Conta linhas com palavra-chave
        if (argc != 4) {
            fprintf(stderr, "Uso incorreto do comando -l\n");
            show_usage(argv[0]);
            return 1;
        }
        
        msg.operation = OP_LINES;
        msg.doc_id = atoi(argv[2]);
        strncpy(msg.keyword, argv[3], MAX_KEYWORD_SIZE - 1);
        
        if (send_receive(&msg, &response, client_pipe) < 0) {
            return 1;
        }
        
        if (response.status == 0) {
            printf("%d\n", response.line_count);
        } else {
            printf("Error: %s\n", response.error_msg);
        }
    }
    else if (strcmp(option, "-s") == 0) {
        // Pesquisa documentos com palavra-chave
        if (argc < 3 || argc > 4) {
            fprintf(stderr, "Uso incorreto do comando -s\n");
            show_usage(argv[0]);
            return 1;
        }
        
        msg.operation = OP_SEARCH;
        strncpy(msg.keyword, argv[2], MAX_KEYWORD_SIZE - 1);
        
        // Verificar se foi especificado o número de processos
        if (argc == 4) {
            msg.nr_processes = atoi(argv[3]);
        } else {
            msg.nr_processes = 1; // Valor padrão
        }
        
        if (send_receive(&msg, &response, client_pipe) < 0) {
            return 1;
        }
        
        if (response.status == 0) {
            // Imprimir lista de IDs
            printf("[");
            for (int i = 0; i < response.doc_count; i++) {
                printf("%d", response.doc_ids[i]);
                if (i < response.doc_count - 1) {
                    printf(", ");
                }
            }
            printf("]\n");
        } else {
            printf("Error: %s\n", response.error_msg);
        }
    }
    else if (strcmp(option, "-f") == 0) {
        // Desligar servidor
        if (argc != 2) {
            fprintf(stderr, "Uso incorreto do comando -f\n");
            show_usage(argv[0]);
            return 1;
        }
        
        msg.operation = OP_SHUTDOWN;
        
        if (send_receive(&msg, &response, client_pipe) < 0) {
            return 1;
        }
        
        if (response.status == 0) {
            printf("Server is shutting down\n");
        } else {
            printf("Error: %s\n", response.error_msg);
        }
    }
    else {
        fprintf(stderr, "Opção desconhecida: %s\n", option);
        show_usage(argv[0]);
        return 1;
    }
    
    return 0;
}