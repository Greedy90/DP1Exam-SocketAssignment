/*
 * Exam of Distributed Programming I, Socket Assignmet
 * Student: Marco Terrinoni
 * ID: 198855
 * Date: 28-06-2013 
 */
 
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <netdb.h>
#include <ctype.h>
#include <signal.h>

#define MAX_NAME 32
#define MAX_SERVER_LIST 10 // maximum number of servers in config file
#define MAX_BUFF 1024 // buffer length
#define SIMULTANEOUS_TH 6 // number of parallel threads
#define BACKLOG 10 // maximum pending request for a single socket
#define MSG_ERR "-ERR\r\n"
#define MSG_OK "+OK\r\n"

//Structs
/**
 * This struct is the elementary block of the server array.
 */
typedef struct server_record {
	char ip_address[MAX_NAME];
	short port;
} server_record_t;

/**
 * This struct is the argument input of the thread.
 */
typedef struct server_arg {
	int server_sock_listen;
	int my_id;
} server_arg_t;

//Prototypes
void import_config(char *filename);
static void *server_child (void *arg);
int get_management (int sockfd, char *filename, int thread_id);
int get_management_to_other_servers (int sockfd, char *filename, int thread_id);
ssize_t send_stuff (int sockfd, void *msg, size_t len);
ssize_t send_file (int sockfd, int fd, long int dim_file);
ssize_t recv_stuff (int sockfd, char *msg);
ssize_t recv_file_size (int sockfd, uint32_t *size);
ssize_t forward_file (int sock_src, int sock_dest, long int dim_file);
void clean_buff (char *buff);

//Global variables
pthread_t server_thread_array[SIMULTANEOUS_TH];
server_arg_t server_thread_argument[SIMULTANEOUS_TH];
server_record_t server_list[MAX_SERVER_LIST];
short server_sock_port;

/**
 * This is the main program.
 */
int main (int argc, char *argv[])
{
	int sock_listen, result, i;
	struct sockaddr_in server_sock_addr;
	
	signal(SIGPIPE, SIG_IGN); // ignore signal pipe

	//Initial check for the input argument
	fprintf(stdout, "Check for initial input arguments...\n");
	if(argc != 3) {
		fprintf(stderr, "ERROR 01: wrong input parameter.\n");
		exit(EXIT_FAILURE);
	}
	fprintf(stdout, "[OK]\n");
	
	//Save the port
	fprintf(stdout, "Save the server port... \n");
	server_sock_port = (short)atoi(argv[1]);
	fprintf(stdout, "[OK]\n");
	
	//Import the configuration file
	fprintf(stdout, "Import the configuration file...\n");
	import_config(argv[2]);
	fprintf(stdout, "[OK]\n");
	
	//Create the server socket
	fprintf(stdout, "Create the socket...\n");
	sock_listen = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(sock_listen == -1) {
		perror("Cannot create the server socket");
		exit(EXIT_FAILURE);
	}
	fprintf(stdout, "[OK]\n");
	
	//Setting the server socket
	fprintf(stdout, "Set the socket...\n");
	server_sock_addr.sin_family = AF_INET; // family
	server_sock_addr.sin_port = htons(server_sock_port); // port
	server_sock_addr.sin_addr.s_addr = htonl(INADDR_ANY); //listening from all IP addresses (0.0.0.0)
	fprintf(stdout, "[OK]\n");
	
	//Bind the socket
	fprintf(stdout, "Bind the socket...\n");
	result = bind(sock_listen, (struct sockaddr *) &server_sock_addr, sizeof(server_sock_addr));
	if(result == -1) {
		perror("Cannot bind the server socket");
		exit(EXIT_FAILURE);
	}
	fprintf(stdout, "[OK]\n");
	
	//Listening
	fprintf(stdout, "Set socket listening...\n");
	result = listen(sock_listen, BACKLOG);
	if(result == -1) {
		perror("Cannot initialize the listen status for the socket");
		exit(EXIT_FAILURE);
	}
	fprintf(stdout, "[OK]\n");
	
	//Creation of the thread
	fprintf(stdout, "Create threads...\n");
	for(i = 1; i <= SIMULTANEOUS_TH; i++) {
		server_thread_argument[i-1].my_id = i;
		server_thread_argument[i-1].server_sock_listen = sock_listen;
		result = pthread_create(&server_thread_array[i-1], NULL, server_child, (void *) &server_thread_argument[i-1]);
		if(result != 0) {
			fprintf(stderr, "ERROR 02: cannot initialize threads.\n");
			exit(EXIT_FAILURE);
		}
	}
	
	//Wait for all threads conclusion
	for(i = 1; i <= SIMULTANEOUS_TH; i++)
		pthread_join(server_thread_array[i-1],NULL);
	
	return(EXIT_SUCCESS);
}

//Functions
/**
 * This function import the configuration file, which name is received as input
 * parameter, and save the entries into the global server_list. If the config file is
 * empty then no element is server list is updated.
 */
void import_config(char *filename)
{
	FILE *fp;
	int i = 0, status, tmp_port;
	char tmp_ip_name[MAX_NAME];
	struct addrinfo *servinfo;
	struct sockaddr_in *ipv;
	struct in_addr *addr;
	
	fp = fopen(filename, "r");
	if(fp == NULL) {
		perror("Cannot open the configuration file");
		exit(EXIT_FAILURE);
	}
	
	while(fscanf(fp, "%s %d", tmp_ip_name, &tmp_port) == 2 && i < MAX_SERVER_LIST) {
		if(isdigit(tmp_ip_name[0])) { // check if the first char of the string is a number
			//Here because the string is actually an IP address
			strcpy(server_list[i].ip_address, tmp_ip_name); // copy the dotted ip address into the server list
			server_list[i].port = (short) tmp_port; // copy the port into the server list
		} else {
			//Here because the string represents the name of the host
			status = getaddrinfo(tmp_ip_name, NULL, NULL, &servinfo); // create the addrinfo structure
			if (status != 0) {
				perror("Cannot get IP address from hostname.");
				fclose(fp);
				exit(EXIT_FAILURE);
			}
			ipv = (struct sockaddr_in *) servinfo->ai_addr; // get the struct socket address from the addrinfo
			addr = &(ipv->sin_addr); // get the ip address from the struct socket address
			inet_ntop(servinfo->ai_family, addr, server_list[i].ip_address, sizeof server_list[i].ip_address); // get the dotted ip address
			server_list[i].port = (short) tmp_port; // copy the port into the server list
			freeaddrinfo(servinfo);
		}
		fprintf(stdout, "Imported: %s %d\n", server_list[i].ip_address, server_list[i].port);
		i++;
	}
	
	if(i == 0)
		fprintf(stdout, "The configuration file is empty.\n");
	
	fclose(fp);
	
	return;
}

/**
 * This function is called by the threads and manages all the functions of the server.
 */
static void *server_child (void *arg)
{
	server_arg_t my_arg = *((server_arg_t *) arg);
	int my_id = my_arg.my_id, server_sock_listen = my_arg.server_sock_listen; // extract the arguments
	char *buff;
	
	//Create the initial buffer
	fprintf(stdout, "Thread %d -> initialize local buffer...\n", my_id);
	buff = (char *) calloc(MAX_BUFF, sizeof(char));
	if(buff == NULL) {
		fprintf(stderr, "ERROR 03: cannot allocate memory for buffer.\n");
		pthread_exit(0);
	}
	fprintf(stdout, "Thread %d -> [OK]\n", my_id);
	
	//Main loop: manage connection
	while(1) {
		int server_sock_connect;
		struct sockaddr_in client_addr;
		socklen_t client_addr_len = sizeof(client_addr);
		
		//Accept connection from client
		fprintf(stdout, "Thread %d -> wait for connection...\n", my_id);
		server_sock_connect = accept(server_sock_listen, (struct sockaddr *) &client_addr, &client_addr_len);
		fprintf(stdout, "Thread %d -> connected\n", my_id);
		
		//Inner loop: manage commands
		while(1) {
			ssize_t nread, nwrite;
			char *filename;
			int result;
		
			//Receive command from client
			fprintf(stdout, "Thread %d -> wait for commands...\n", my_id);
			clean_buff(buff);
			nread = recv_stuff(server_sock_connect, buff);
			if(nread < 0) {
				fprintf(stderr, "ERROR 04: Error during receive command.\n");
				free(buff);
				pthread_exit(0);
			}
			fprintf(stdout, "Thread %d -> command received: %s\n", my_id, buff);
			
			//Command selection
			if(strncmp(buff, "GET", 3) == 0) {
				//GET command received
				filename = strdup(buff + 3); // start from the fourth position
				if(filename == NULL) {
					fprintf(stderr, "ERROR 05: cannot allocate save filename.\n");
					free(buff);
					pthread_exit(0);
				}
				result = get_management(server_sock_connect, filename, my_id);
				if(result != 1) {
					//The file cannot be obtained, send -ERR
					clean_buff(buff);
					if(result != -2) {
						nwrite = send_stuff(server_sock_connect, MSG_ERR, strlen(MSG_ERR));
						if(nwrite < 0) {
							fprintf(stderr, "ERROR 06: cannot send the error message.\n");
							free(buff);
							pthread_exit(0);
						}
					}
					break;
				}
			} else if(strncmp(buff, "QUIT", 4) == 0) {
				//QUIT command received
				break;
			} else {
				//Invalid command received, send -ERR
				clean_buff(buff);
				nwrite = send_stuff(server_sock_connect, MSG_ERR, strlen(MSG_ERR));
				if(nwrite < 0) {
					fprintf(stderr, "ERROR 07: cannot send the error message.\n");
					free(buff);
					pthread_exit(0);
				}
				break;
			}
		}
		
		//Close connection
		close(server_sock_connect);
	}
	
	free(buff);
	
	pthread_exit(0);
}

/**
 * This function is called by the thread server_child and it manages the different
 * behaviour with file selection: if the server has the file, this function open the file
 * and sends it to the client, otherwise it calls another function that try to invoke the
 * other servers in the server list. The function returns -1 in case of system error, -2
 * in case of send/recv error 1 in case of correct sent.
 */
int get_management (int sockfd, char *filename, int thread_id)
{
	int fd;
	long int dim_file;
	char *buff;
	ssize_t nwrite;
	struct stat stat_buf;
	uint32_t dim_file_net;
	
	//Create the initial buffer
	buff = (char *) malloc(MAX_BUFF * sizeof(char));
	if(buff == NULL) {
		fprintf(stderr, "Thread %d -> ERROR 08: cannot allocate memory for buffer.\n", thread_id);
		return(-1);
	}
	
	fprintf(stdout, "Thread %d -> file selected: %s\n", thread_id, filename);
	fd = open(filename, O_RDONLY);
	if(fd == -1) {
		//Here because the server doesn't have the file
		fprintf(stdout, "Thread %d -> file doesn't exist\n", thread_id);
		free(buff);
		close(fd);
		return(get_management_to_other_servers(sockfd, filename, thread_id)); //<---
	} else {
		//Here because the server has the file
		//Send "+OK"
		fprintf(stdout, "Thread %d -> the file exists, sending notification...\n", thread_id);
		clean_buff(buff);
		nwrite = send_stuff(sockfd, MSG_OK, strlen(MSG_OK));
		if(nwrite < 0) {
			fprintf(stderr, "Thread %d -> ERROR 09: error during sending \"+OK\" reply.\n", thread_id);
			free(buff);
			return(-1);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Look for the information of the selected file
		fprintf(stdout, "Thread %d -> get the information of the file...\n", thread_id);
		if (fstat(fd, &stat_buf) == -1) {
			perror("Cannot open file descriptor");
			free(buff);
			close(fd);
			return(-1);
		}
		dim_file = stat_buf.st_size; //dim_file contains the dimension of the file
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Cast the length from int to the 32-bit integer network format
		dim_file_net = htonl((uint32_t) dim_file);
		
		//Send the dimension of the selected file
		fprintf(stdout, "Thread %d -> send dimension of the file...\n", thread_id);
		clean_buff(buff);
		nwrite = send_stuff(sockfd, &dim_file_net, sizeof(dim_file_net));
		if(nwrite < 0) {
			fprintf(stderr, "Thread %d -> ERROR 10: error during sending information about the file.\n", thread_id);
			free(buff);
			close(fd);
			return(-2);
		}
		fprintf(stdout, "Thread %d -> dimension sent, value:%ld\n", thread_id, dim_file);
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Send the selected file
		fprintf(stdout, "Thread %d -> send the selected file...\n", thread_id);
		clean_buff(buff);
		nwrite = send_file(sockfd, fd, dim_file);
		if(nwrite != dim_file) {
			fprintf(stderr, "Thread %d -> ERROR 11: error during sending the file.\n", thread_id);
			return(-2);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		free(buff);
		close(fd);
		
		return(1); // return status for "I have sent the file"
	}
}

/**
 * This is called by get_management and it's in charge of managing the behaviour of the
 * unexisting file, so it calls the other servers in the server_list, and it becomes a
 * sort of client with that servers and if one of them have the file, this function
 * forward the received stream to the original client. It returns -1 in case of system
 * error, -2 in case of send/recv error, 1 in case of success.
 */
int get_management_to_other_servers (int sockfd, char *filename, int thread_id)
{
	int flag = 0, i = 0;
	char *buff;
	
	//Create the initial buffer
	buff = (char *) calloc(MAX_BUFF, sizeof(char));
	if(buff == NULL) {
		fprintf(stderr, "Thread %d -> ERROR 12: cannot allocate memory for buffer.\n", thread_id);
		return(-1);
	}
	
	//Searching loop
	while(flag == 0 && i <= MAX_SERVER_LIST) {
		struct in_addr remote_server_ip_addr;
		short remote_server_port;
		int tmp_remote_server_sock;
		long int dim_file;
		struct sockaddr_in tmp_remote_server_addr;
		ssize_t nread, nwrite;
		uint32_t dim_file_net;
		
		fprintf(stdout, "Thread %d -> try to contact another server\n", thread_id);
		
		//Extract the ip address of the current server from the server_list
		fprintf(stdout, "Thread %d -> Extract the element from the server list...\n", thread_id);
		if(!inet_aton(server_list[i].ip_address, &remote_server_ip_addr)) {
			fprintf(stderr, "Thread %d -> extraction failed\n", thread_id);
			break; // error during extraction, terminate the searching loop
		}
		remote_server_port = server_list[i].port;
		fprintf(stdout, "Thread %d -> Extracted: %s %d\n", thread_id, inet_ntoa(remote_server_ip_addr), remote_server_port);
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Creation of the socket
		fprintf(stdout, "Thread %d -> creation of the socket for contact the selected server\n", thread_id);
		tmp_remote_server_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if(tmp_remote_server_sock == -1) {
			fprintf(stderr, "Thread %d -> Cannot create the socket\n", thread_id);
			return(-1);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Setting the socket
		fprintf(stdout, "Thread %d -> set the socket for contact the selected server\n", thread_id);
		tmp_remote_server_addr.sin_family = AF_INET;
		tmp_remote_server_addr.sin_port = htons(remote_server_port);
		tmp_remote_server_addr.sin_addr = remote_server_ip_addr;
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Try to contact the selected server
		fprintf(stdout, "Thread %d -> try to contact %s : %d\n", thread_id, inet_ntoa(remote_server_ip_addr), remote_server_port);
		if(connect(tmp_remote_server_sock, (struct sockaddr *) &tmp_remote_server_addr, sizeof(tmp_remote_server_addr)) == -1) {
			fprintf(stderr, "Thread %d -> Connection failed\n", thread_id);
			i++;
			continue; // error during connection or server unreachable, go to the next iteration
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Creation of the command
		fprintf(stdout, "Thread %d -> creation of the command\n", thread_id);
		clean_buff(buff);
		sprintf(buff, "GET");
		buff[strlen(buff) + 1] = '0';
		strcat(buff, filename);
		strcat(buff, "\r\n");
		fprintf(stdout, "Thread %d -> generated command: %s\n", thread_id, buff);
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Send command
		fprintf(stdout, "Thread %d -> send the command\n", thread_id);
		nwrite = send_stuff(tmp_remote_server_sock, buff, strlen(buff));
		if(nwrite < 0) {
			fprintf(stderr, "Thread %d -> Error during send the command.\n", thread_id);
			return(-2);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Receive the reply
		clean_buff(buff);
		fprintf(stdout, "Thread %d -> wait for the reply...\n", thread_id);
		nread = recv_stuff(tmp_remote_server_sock, buff);
		if(nread < 0) {
			fprintf(stderr, "Thread %d -> ERROR 13: error during receiving the reply.\n", thread_id);
			break;
		}
		
		//Print the reply
		fprintf(stdout, "Thread %d -> message received: %s\n", thread_id, buff);
		if(strncmp(buff, "-ERR", 4) == 0) {
			i++;
			continue; // the server doesn't have the selected file, go to the next iteration
		}
		
		//Forward the reply
		fprintf(stdout, "Thread %d -> forward the reply...\n", thread_id);
		//nwrite = send_stuff(sockfd, buff, strlen(buff));
		nwrite = send_stuff(sockfd, MSG_OK, strlen(MSG_OK));
		if(nwrite < 0) {
			fprintf(stderr, "Thread %d -> Error during send the command.\n", thread_id);
			return(-2);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Receive the size of the file
		fprintf(stdout, "Thread %d -> receive the size of the file...\n", thread_id);
		clean_buff(buff);
		nread = recv_file_size(tmp_remote_server_sock, &dim_file_net);
		dim_file = ntohl(dim_file_net); // conversion from network format to host format
		fprintf(stdout, "Thread %d -> the size of the file is %ld\n", thread_id, dim_file);
		if(nread < 0) {
			fprintf(stderr, "ERROR 14: error during receiving the dimension of the selected file.\n");
			break;
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Forward the size
		fprintf(stdout, "Thread %d -> forward the size of the file...\n", thread_id);
		nwrite = send_stuff(sockfd, &dim_file_net, sizeof(dim_file_net));
		if(nwrite < 0) {
			fprintf(stderr, "Thread %d -> ERROR 15: Error during forward the size of the file\n", thread_id);
			return(-2);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Forward the file
		fprintf(stdout, "Thread %d -> forward the the file...\n", thread_id);
		nwrite = forward_file(tmp_remote_server_sock, sockfd, dim_file);
		if(nwrite < dim_file) {
			fprintf(stdout, "Thread %d -> some problems occurs during send file\n", thread_id);
			return(-2);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Creation of quit the command
		clean_buff(buff);
		fprintf(stdout, "Thread %d -> creation of the quit command\n", thread_id);
		sprintf(buff, "QUIT\r\n");
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		//Send quit command to the server
		fprintf(stdout, "Thread %d -> send command...\n", thread_id);
		nwrite = send_stuff(tmp_remote_server_sock, buff, strlen(buff));
		if(nwrite < 0) {
			fprintf(stderr, "Thread %d -> ERROR 16: error during send the command\n", thread_id);
			return(-2);
		}
		fprintf(stdout, "Thread %d -> [OK]\n", thread_id);
		
		flag = 1; // update the flag
		
		close(tmp_remote_server_sock);
	}
	
	//Check result of the search
	if(flag == 1) { // true
		//The forwarding is completed
		free(buff);
		return(1);
	} else { // false
		//Some error occurs or the servers doesn't have the selected file
		free(buff);
		return(-1);
	}
}

/**
 * This function receives in input the descriptor of the socket (int sockfd) in which
 * send the message (void *msg), with fixed length (size_t len). It returns the total
 * amount of sent byte.
 */
ssize_t send_stuff (int sockfd, void *msg, size_t len)
{
	ssize_t nleft, nwrite;
	
	for(nleft = len; nleft > 0; ) {
		nwrite = send(sockfd, msg, nleft, 0);
		if(nwrite <= 0)
			return(nwrite);
		else {
			nleft -= nwrite;
			msg += nwrite;
		}
	}
	
	return(len - nleft);
}

/**
 * This function sends the file (file descriptor int fd) with a specific dimension (int
 * dim_file) into the selected socket (int sockfd), by using a local buffer. The function
 * returns the number of actually byte sent.
 */
ssize_t send_file (int sockfd, int fd, long int dim_file)
{
	int nwrite, nread;
	long int  tot_write = 0;
	char *buff;
	
	buff = (char *) malloc(MAX_BUFF * sizeof(char));
	if(buff == NULL) {
		fprintf(stderr, "ERROR 17: cannot allocate memory for buffer.\n");
		return(-1);
	}
	clean_buff(buff);
	
	while((nread = read(fd, buff, MAX_BUFF)) > 0 && tot_write <= dim_file) {
		nwrite = send_stuff(sockfd, buff, nread);
		clean_buff(buff);
		tot_write += (long int) nwrite;
		if(tot_write == dim_file)
			break;
		if(nwrite < 0)
			return(nwrite);
	}
	
	return(tot_write);
}

/**
 * This function receives in input the descriptor of the socket (int sockfd) in which it
 * receives the message (saved into a string pointed by "msg"). It returns the number of
 * byte actually received.
 */
ssize_t recv_stuff (int sockfd, char *msg)
{
	int nread = 0;
	char acc_char;
	
	while ((recv(sockfd, &acc_char, 1, 0) > 0) && (acc_char != '\n') && (nread < MAX_BUFF)) {
		if ((acc_char != '\r') && (acc_char != '\n')) {	
			msg[nread] = acc_char;
			nread++;
		}
	}
	msg[nread] = '\0';
	
	return(nread);
}

/**
 * This function is a variation of the previous recv_stuff and it's specific for
 * receiving the size of the files directly in a network format, without buffers.
 */
ssize_t recv_file_size (int sockfd, uint32_t *size)
{
	int nread = 0;
	
	nread = recv(sockfd, size, sizeof(size), 0);
	
	return(nread);
}

/**
 * This function forward the stream coming from sock_src to sock_dest, for a maximum of
 * dim_file.
 */
ssize_t forward_file (int sock_src, int sock_dest, long int dim_file)
{
	char *buff;
	int nread, nwrite;
	long int tot_sent = 0;
	
	buff = (char *) calloc(MAX_BUFF, sizeof(char));
	if(buff == NULL) {
		fprintf(stderr, "ERROR 18: cannot allocate memory for buffer.\n");
		return(-1);
	}
	
	while((nread = recv(sock_src, buff, MAX_BUFF, 0)) > 0 && tot_sent < dim_file) {
		if(nread == -1) // in case of errors during recv
			return(nread);
		nwrite = send_stuff(sock_dest, buff, nread);
		clean_buff(buff);
		tot_sent += (long int) nwrite;
		if(tot_sent == dim_file)
			return(tot_sent);
		if(nwrite < 0)
			return(nwrite);			
	}
	
	return(tot_sent);
}

/**
 * This function simply cleans the buffer by using the memset function.
 */
void clean_buff (char *buff)
{	
	memset(buff, 0, MAX_BUFF);
	
	return;
}

