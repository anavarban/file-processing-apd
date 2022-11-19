#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <math.h>

#define NAME_SIZE 40
#define MAX_FILE_NO 3000
#define MAX_SIZE 500000

pthread_barrier_t barrier;
pthread_mutex_t mutex;

//structura pentru pasarea argumentelor functiei de thread
typedef struct {
    char **file_names;
    int file_number, max_exp, num_threads, id, M;
    int ***partial_lists;
    int **index_record_mat;
} Arguments;

int min(int a, int b) {
    if (a < b) {
        return a;
    }
    return b;
}

void *thread_task(void *arg) {
    Arguments* args = (Arguments *)arg;
    int N = args->file_number;
    int P = args->M;
    int ID = args->id;

    int start = ID * (double)N / P;
    int end = min((ID + 1) * (double)N / P, N);

    //alias pt index_record-ul mapperului
    int *index_record = (int*)(args->index_record_mat)[ID];

    //daca threadul este mapper, incepe map
    //(mapperii vor fi primele M threaduri (dupa index))
    if (ID < P) {
        for (int i = start; i < end; i++) {
            //threadul curent se va ocupa de fisierele dintre start si end
            FILE* input = fopen(args->file_names[i], "r");
            int no_numbers;
            fscanf(input, "%d", &no_numbers);
            for (int j = 0; j < no_numbers; j++) {
                int crt_number;
                fscanf(input, "%d", &crt_number);
                if (crt_number >= 1) {
                    
                    //logica pt verificare putere perfecta
                    //se bazeaza pe cautare binara
                    for (int q = 2; q <= args->max_exp; q++) {
                        int mid = 0;
                        int upper_bound = (int)sqrt(crt_number);
                        for (int k = 1; k <= upper_bound; ) {
                            mid = (upper_bound + k) / 2;
                            if (crt_number == (int)pow(mid, q)) {
                                //pthread_mutex_lock(&mutex);
                                args->partial_lists[ID][q][index_record[q]] = crt_number;
                                index_record[q]++;
                                //pthread_mutex_unlock(&mutex);
                                break;
                            }
                            else if( k == upper_bound)
                            {
                                break;
                            }
                            if (crt_number < (int)pow(mid, q)) {
                                upper_bound = mid - 1;
                            }
                            if (crt_number > (int)pow(mid, q)){
                                k = mid +1;
                            }
                        }
                    }


                }
            }
            fclose(input);
        }
    }
    //s-a terminat map
    //bariera, pentru ca toate threadurile mapper sa termine
    pthread_barrier_wait(&barrier);


    //nu uita sa stergi!!!!
    // for (int i = 2; i < args->max_exp + 2; i++) {
    //     for (int k = 0; k < (args->index_record_mat)[ID][args->id + 2]; k++) {
    //         printf("partial_list[%d][%d][%d] = %d\n", ID, i, k, args->partial_lists[ID][i][k]);
    //     }
    // }


    //incepe reduce

    //retinem o lista de valori ce au aparut in lista partiala
    int *partial_reduced = calloc(MAX_SIZE, sizeof(int));
    int count = 0;
    for (int i = 0; i < args->M; i++) {
        for (int k = 0; k < (args->index_record_mat)[i][args->id + 2]; k++) {
            int found = 0;
            //verificam daca valoarea curenta din lista partiala a fost gasita deja
            for (int j = 0; j < count; j++) {
                if(partial_reduced[j] == args->partial_lists[i][args->id + 2][k]) {
                    found = 1;
                    j = count;
                }
            }
            //daca nu a fost gasita o adaugam si o numaram
            if(!found) {
                partial_reduced[count] = args->partial_lists[i][args->id + 2][k];
                count++;
            }
        }
    }

    //nu uita sa stergi!!!
    for (int i = 0; i < count; i++) {
        printf("partial_reduced[%d] = %d in thread %d\n", i, partial_reduced[i], ID);
    }

    //afisam rezultatul reduce in fisierul de output aferent
    char out[NAME_SIZE];
    sprintf(out, "out%d.txt", args->id + 2);
    FILE* output = fopen(out, "w");
    fprintf(output, "%d", count);
    fclose(output);

    pthread_exit(NULL);

}


int max(int a, int b) {
    if (a > b) {
        return a;
    }
    return b;
}

int main(int argc, char* argv[]) {
    //preluare argumente
    if (argc < 4) {
        printf("Numar insuficient de parametri.\n Usage: ./tema1 no_mapperi no_reduceri fisier_intrare\n");
        exit(1);

    }
    int M = atoi(argv[1]);
    int R = atoi(argv[2]);
    char input_file[NAME_SIZE];
    strcpy(input_file, argv[3]);

    FILE *in = fopen(input_file, "r");
    int file_number;

    //array de string-uri ce pastreaza numele fisierelor
    //de input

    char** file_names = calloc(MAX_FILE_NO, sizeof(char*));
    for (int i = 0; i < MAX_FILE_NO; i++) {
        file_names[i] = calloc(NAME_SIZE, sizeof(char));
    }

    //citim numele fisierelor din input si le salvam
    fscanf(in, "%d", &file_number);
    for (int i = 0; i < file_number; i++) {
        fscanf(in, "%s", file_names[i]);
    }

    int max_exp = R + 1;
    //vom crea thread-uri in numar de maxim(nr_mapperi, nr_reduceri)
    int num_threads = max(M, R);
    pthread_t threads[num_threads];

    
    //index record pentru a salva dimensiunea listelor partiale 
    //ale fiecarui mapper
    int** index_record_mat = calloc(num_threads, sizeof(int*));
    for (int i = 0; i < num_threads; i++){
        index_record_mat[i] = calloc(max_exp + 2, sizeof(int));
    }

    //retinem listele partiale ca tablou tridimensional

    int*** partial_lists = calloc(num_threads, sizeof(int**));
    for (int i = 0; i < num_threads; i++) {
        partial_lists[i] = calloc(max_exp + 2, sizeof(int*));
        for (int j = 0; j < max_exp + 2; j++) {
            partial_lists[i][j] = calloc(MAX_SIZE, sizeof(int));
        }
    }

    //initializare bariera
    pthread_barrier_init(&barrier, NULL, num_threads);
    pthread_mutex_init(&mutex, NULL);


    Arguments args[num_threads];
    int r;
    void *status;
    
    //creare threaduri
    for (int i = 0; i < num_threads; i++) {
        args[i].id = i;
        args[i].file_names = (char**)file_names;
        args[i].file_number = file_number;
        args[i].M = M;
        args[i].max_exp = max_exp;
        args[i].num_threads = num_threads;
        args[i].partial_lists = (int***)partial_lists;
        args[i].index_record_mat = (int**)index_record_mat;

        r = pthread_create(&threads[i], NULL, thread_task, &args[i]);
        if (r) {
			printf("Eroare la crearea thread-ului %d\n", i);
			exit(-1);
		}

    }
    //join threaduri
    for (int i = 0; i < num_threads; i++) {
		r = pthread_join(threads[i], &status);

		if (r) {
			printf("Eroare la asteptarea thread-ului %d\n", i);
			exit(-1);
		}
	}


    //eliberare memorie

    for (int i = 0; i < MAX_FILE_NO; i++) {
        free(file_names[i]);
    }
    free(file_names);


    for (int i = 0; i < num_threads; i++) {
        for (int j = 0; j < max_exp + 2; j++) {
            free(partial_lists[i][j]);
        }
        free(partial_lists[i]);
    }
    free(partial_lists);

    for (int i = 0; i < num_threads; i++){
        free(index_record_mat[i]);
    }
    free(index_record_mat);

    pthread_barrier_destroy(&barrier);
	pthread_mutex_destroy(&mutex);
    fclose(in);

}