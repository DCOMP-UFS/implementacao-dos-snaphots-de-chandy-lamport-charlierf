/* File:  
 *    etapa4.c
 *
 * Purpose:
 *    Implementação do algoritmo de chandy-lamport usando relógios vetoriais
 *
 *
 *
 * Compile:  mpicc -g -Wall -o etapa4 etapa4.c -lpthread -lrt
 * Usage:    mpiexec -n 3 ./etapa4
 *
 *Charlie Rodrigues Fonseca
 *Elana Tanan Sande
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h> 
#include <unistd.h>
#include <semaphore.h>
#include <time.h>
#include <mpi.h>     

#define THREAD_NUM 9    // Tamanho do pool de threads
#define BUFFER_SIZE 12 // Númermo máximo de tarefas enfileiradas


typedef struct {
    int p[3];
    int flag;
} Clock;

typedef struct {
    int id;
    Clock clock;  // Snapshot data structure to hold the state
    int snapshotTaken;  // Flag to indicate if a snapshot has been taken
} Process;

typedef struct {
    Clock clock;
    Clock in[BUFFER_SIZE];
    Clock out[BUFFER_SIZE];
} Snapshot;

pthread_mutex_t outMutex;
pthread_cond_t outCondEmpty;
pthread_cond_t outCondFull;
int outClockCount = 0;
Clock outClockQueue[BUFFER_SIZE];

pthread_mutex_t inMutex;
pthread_cond_t inCondEmpty;
pthread_cond_t inCondFull;
int inClockCount = 0;
Clock inClockQueue[BUFFER_SIZE];


void CompareClock(Clock* clock, Clock* clock1){
    if (clock->p[0] < clock1->p[0]) { clock->p[0] = clock1->p[0]; }
    if (clock->p[1] < clock1->p[1]) { clock->p[1] = clock1->p[1]; }
    if (clock->p[2] < clock1->p[2]) { clock->p[2] = clock1->p[2]; }
}

void Event(int pid, Clock *clock) {
    clock->p[pid]++;
    printf("Process: %d, Clock: (%d, %d, %d)\n", pid, clock->p[0], clock->p[1], clock->p[2]);
}

Clock GetClock(pthread_mutex_t *mutex, pthread_cond_t *condEmpty, pthread_cond_t *condFull, int *clockCount, Clock *clockQueue) {
    Clock clock;
    pthread_mutex_lock(mutex);
    
    while (*clockCount == 0) {
        pthread_cond_wait(condEmpty, mutex);
    }

    clock = clockQueue[0];

    for (int i = 0; i < *clockCount - 1; i++) {
        clockQueue[i] = clockQueue[i + 1];
    }

    (*clockCount)--;
    
    pthread_mutex_unlock(mutex);

    pthread_cond_signal(condFull);
    
    return clock;
}



void PutClock(pthread_mutex_t *mutex, pthread_cond_t *condEmpty, pthread_cond_t *condFull, int *clockCount, Clock clock, Clock *clockQueue) {
    pthread_mutex_lock(mutex);

    while (*clockCount == BUFFER_SIZE) {
        pthread_cond_wait(condFull, mutex);
    }
    
    Clock temp = clock;

    clockQueue[*clockCount] = temp;
    (*clockCount)++;
    

    pthread_mutex_unlock(mutex);
    pthread_cond_signal(condEmpty);
}

void InitiateSnapshot(Process* process) {
    //Inicia o Snapshot
    Snapshot snapshot;
    snapshot.clock = process->clock;
    for (int i = 0; i < BUFFER_SIZE; i++) {
        snapshot.in[i] = inClockQueue[i];
        snapshot.out[i] = outClockQueue[i];
    }
    process->snapshotTaken = 1; //Sinaliza na estrutura que o processo já fez snapshot
    
    Clock *clock = malloc(sizeof(Clock));
    clock->p[0] = -1;
    clock->p[1] = -1;
    clock->p[2] = -1;

    //Imprime o Snapshot (Relógio, canal de entra e canal de saída)
    printf("Snapshot iniciado no processo %d. Clock: (%d, %d, %d)\n",
           process->id, process->clock.p[0], process->clock.p[1], process->clock.p[2]);
    printf("Canal de Entrada: ");
    for (int i = 0; i < inClockCount; i++){
        printf("[(%d, %d, %d)]  ", snapshot.in[i].p[0], snapshot.in[i].p[1], snapshot.in[i].p[2]);
    }
    printf("\nCanal de Saída: ");
    for (int i = 0; i < outClockCount; i++){
        printf("[(%d, %d, %d)->%d]  ", snapshot.out[i].p[0], snapshot.out[i].p[1], snapshot.out[i].p[2], snapshot.out[i].flag);
    }
    printf("\n");
    
    //Envia marcadores (-1) para os outros processos
    for (int i = 0; i < 3; i++){
        if (i != process->id){
            clock->flag = i;
            PutClock(&outMutex, &outCondEmpty, &outCondFull, &outClockCount, *clock, outClockQueue);
        }
    }
    clock->flag = -2; //Flag -2 para sinalizar à thread send que o snapshot ocorreu
    PutClock(&outMutex, &outCondEmpty, &outCondFull, &outClockCount, *clock, outClockQueue);
    
    free(clock);
}


void SendControl(int id, pthread_mutex_t* mutex, pthread_cond_t* condEmpty, pthread_cond_t* condFull,
                 int* clockCount, Clock* clock, Clock* clockQueue, Process* processes) {
    Event(id, clock);

    PutClock(mutex, condEmpty, condFull, clockCount, *clock, clockQueue);
}

Clock* ReceiveControl(int id, pthread_mutex_t* mutex, pthread_cond_t* condEmpty, pthread_cond_t* condFull,
                      int* clockCount, Clock* clockQueue, Clock* clock, Process* processes) {
    Clock* temp = clock;

    Clock clock2 = GetClock(mutex, condEmpty, condFull, clockCount, clockQueue);
    
    if (clock2.flag == -1) { temp = &clock2; return temp; } //Se receber um marcador, retorna o marcador para sinalizar à thread Main que um marcador foi recebido
    
    CompareClock(temp, &clock2);
    temp->p[id]++;

    printf("Process: %d, Clock: (%d, %d, %d)\n", id, clock->p[0], clock->p[1], clock->p[2]);

    return temp;
}


void Send(int pid, Clock *clock){
   int mensagem[3];
   mensagem[0] = clock->p[0];
   mensagem[1] = clock->p[1];
   mensagem[2] = clock->p[2];
   //MPI SEND
   MPI_Send(&mensagem, 3, MPI_INT, clock->flag, 0, MPI_COMM_WORLD);
   printf("%d -> (%d, %d, %d) -> %d\n", pid, mensagem[0], mensagem[1], mensagem[2], clock->flag);
}

void SendClock(int pid){
  Clock clock = GetClock(&outMutex, &outCondEmpty, &outCondFull, &outClockCount, outClockQueue);
  Send(pid, &clock);
}

Clock Receive(int pid){
    int mensagem[3];
    Clock clock;
    MPI_Status status;
    //MPI RECV
    MPI_Recv(&mensagem, 3, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
    clock.p[0] = mensagem[0];
    clock.p[1] = mensagem[1];
    clock.p[2] = mensagem[2];
    if(clock.p[0] == -1){clock.flag = -1;}
    printf("%d <- (%d, %d, %d) <- %d\n", pid, mensagem[0], mensagem[1], mensagem[2], status.MPI_SOURCE);
    return clock;
}

int ChooseRandomRecipient(int sender, int numProcesses) {
    int recipient;
    do {
        recipient = rand() % numProcesses;
    } while (recipient == sender);
    return recipient;
}



void *MainThread(void *args) {
    long id = (long) args;
    int pid = (int) id;
    Process* process = malloc(sizeof(Process));
    process->id = pid;
    memset(&(process->clock), 0, sizeof(Clock));
    process->snapshotTaken = 0;
    Clock *receivedClock;
    
    // Inicializa a semente do gerador de números aleatórios com base no tempo
    srand(time(NULL));

    int marker = 0;

    //Executa Eventos, Envios, Recepções e o Snapshot aleatoriamente até receber dois marcadores
    while(marker != 2) {
        int randomEvent = rand() % 4; // Gera um número aleatório de 0 a 3
        sleep(randomEvent);
        switch (randomEvent) {
            // Event
            case 0:
                Event(pid, &(process->clock));
                break;
            
            // Send
            case 1:
                if(outClockCount != BUFFER_SIZE){
                    process->clock.flag = ChooseRandomRecipient(pid, 3);
                    SendControl(pid, &outMutex, &outCondEmpty, &outCondFull, &outClockCount, &(process->clock), outClockQueue, process);
                }
                break;
            
            // Receive
            case 2:
                if(inClockCount != 0){
                    receivedClock = ReceiveControl(pid, &inMutex, &inCondEmpty, &inCondFull, &inClockCount, inClockQueue, &(process->clock), process);
                    
                    //Se receber um marcador, incrementa o contador
                    if(receivedClock->flag == -1){
                        marker++;
                        //Se o snapshot ainda não foi iniciado, inicia o snapshot
                        if(!process->snapshotTaken){ 
                            InitiateSnapshot(process);
                            process->snapshotTaken = 1;
                        }
                    //Se não receber um marcador, atualiza o relógio do processo
                    } else {
                        process->clock = *receivedClock;
                    }
                }
                break;
            
            // Snapshot
            case 3:
                //Se ainda não foi iniciado o Snapshot, inicia o Snapshot
                if (!process->snapshotTaken && ((rand()%5) == 1)) {
                    InitiateSnapshot(process);
                    process->snapshotTaken = 1;
                }
                break;
        }
    }
    free(process);
    return NULL;
}

void *SendThread(void *args) {
    long pid = (long) args;
    Clock clock;
    
    while(1){
      clock = GetClock(&outMutex, &outCondEmpty, &outCondFull, &outClockCount, outClockQueue);
      if (clock.flag == -2){
          printf("Snapshot do processo %ld foi finalizado.\n", pid);
          break;
      }
      Send(pid, &clock);
    }
    //printf("Tchau Send %ld\n", pid);
    return NULL;
}


void *ReceiveThread(void *args) {
    long pid = (long) args;
    Clock clock;
    int markerCount = 0;
    
    while(1){
    //while(markerCount != 2){
      clock = Receive(pid);
      PutClock(&inMutex, &inCondEmpty, &inCondFull, &inClockCount, clock, inClockQueue);
      if(clock.flag == -1){
          markerCount++;
      }
      //printf("MarkerCount = %d\n", markerCount);
      //if (markerCount == 2) { break;}
    }
    //printf("Tchau receive %ld\n", pid);
    return NULL;
}


// Representa o processo de rank 0
void process0(){
   Process processes[3];

    for (int i = 0; i < 3; i++) {
        processes[i].id = i;
        memset(&(processes[i].clock), 0, sizeof(Clock));
        processes[i].snapshotTaken = 0;
    }

   pthread_t thread[3];
   pthread_create(&thread[0], NULL, &MainThread, (void*) 0);
   pthread_create(&thread[1], NULL, &SendThread, (void*) 0);
   pthread_create(&thread[2], NULL, &ReceiveThread, (void*) 0);
   
   for (int i = 0; i < 3; i++){  
      if (pthread_join(thread[i], NULL) != 0) {
         perror("Failed to join the thread");
      } else {
          printf("Thread %d from process 0 joined\n", i);
      }
   }
   srand(time(NULL));
   
}

// Representa o processo de rank 1
void process1(){
    Process processes[3];

    for (int i = 0; i < 3; i++) {
        processes[i].id = i;
        memset(&(processes[i].clock), 0, sizeof(Clock));
        processes[i].snapshotTaken = 0;
    }
    
   pthread_t thread[3];
   pthread_create(&thread[0], NULL, &MainThread, (void*) 1);
   pthread_create(&thread[1], NULL, &SendThread, (void*) 1);
   pthread_create(&thread[2], NULL, &ReceiveThread, (void*) 1);
   
   for (int i = 0; i < THREAD_NUM; i++){  
      if (pthread_join(thread[i], NULL) != 0) {
         perror("Failed to join the thread");
      }else {
          printf("Thread %d from process 1 joined\n", i);
      }
   }
   srand(time(NULL));
   
}

// Representa o processo de rank 2
void process2(){
   Process processes[3];

    for (int i = 0; i < 3; i++) {
        processes[i].id = i;
        memset(&(processes[i].clock), 0, sizeof(Clock));
        processes[i].snapshotTaken = 0;
    }
    
   pthread_t thread[3];
   pthread_create(&thread[0], NULL, &MainThread, (void*) 2);
   pthread_create(&thread[1], NULL, &SendThread, (void*) 2);
   pthread_create(&thread[2], NULL, &ReceiveThread, (void*) 2);
   
   for (int i = 0; i < 3; i++){  
      if (pthread_join(thread[i], NULL) != 0) {
         perror("Failed to join the thread");
      }else {
          printf("Thread %d from process 2 joined\n", i);
      }
   }
   
   srand(time(NULL));
   
}


/*--------------------------------------------------------------------*/
int main(int argc, char* argv[]) {
   int my_rank;
   
   pthread_mutex_init(&inMutex, NULL);
   pthread_mutex_init(&outMutex, NULL);
   pthread_cond_init(&inCondEmpty, NULL);
   pthread_cond_init(&outCondEmpty, NULL);
   pthread_cond_init(&inCondFull, NULL);
   pthread_cond_init(&outCondFull, NULL);
   
   srand(time(NULL));
   
   MPI_Init(NULL, NULL); 
   MPI_Comm_rank(MPI_COMM_WORLD, &my_rank); 

   if (my_rank == 0) { 
      process0();
   } else if (my_rank == 1) {  
      process1();
   } else if (my_rank == 2) {  
      process2();
   }

   /* Finaliza MPI */
   
   
      pthread_mutex_destroy(&inMutex);
      pthread_mutex_destroy(&outMutex);
      pthread_cond_destroy(&inCondEmpty);
      pthread_cond_destroy(&outCondEmpty);
      pthread_cond_destroy(&inCondFull);
      pthread_cond_destroy(&outCondFull);
   
   MPI_Finalize();

   return 0;
} 