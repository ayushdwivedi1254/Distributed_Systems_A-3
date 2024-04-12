# Assignment 2: Implementing a Scalable Database with Sharding

## Group Members:
### [Ayush Kumar Dwivedi](https://github.com/ayushdwivedi1254/) (20CS10084)  
### [Saptarshi De Chaudhury](https://github.com/saptarshidec) (20CS10080)  
### [Nikumbh Sarthak Sham](https://github.com/sarthak-nik) (20CS30035)  

## Prerequisites
Ensure that Docker and Docker Compose are installed on your machine before proceeding.  

## Usage
### Cleaning Up

To clean up existing containers and images related to the Load Balancer project, execute the following command:  

``` make clean ```

### Running the Setup

To build and deploy all the docker images and containers and get the system up and running, execute the following command:  

``` make run ```

This command will automatically clean up existing containers and images and start the application.  

## Design Choices

The server and load balancer have been implemented as Flask applications, containerized and deployed using Docker.  

### Consistent Hashing Implementation

A class named ConsistentHashing has been defined which contains all the necessary functions related to mapping of request and server using the consistent hashing scheme.  

#### Data Structures used 

`serverList`: It is an array of size **M (512)**, each index storing the name of the server that is mapped to that slot.  
`serverIndices`: Maintains the virtual server indexes of all the servers as a sorted set. It represents the circular structure of the consistent hash map.   
`serverNameToIndex`: This is a dictionary object. For each server, it maintains a list of virtual server indexes that it is mapped to.  


#### Working of Consistent Hashing

Whenever a server is added to the network, its corresponding virtual server indexes are computed and stored in the respective data structures. Quadratic probing is used to ensure that two virtual server indexes do not coincide.  

Now when a request is received, the hash of its request ID is computed. We take the upper bound of the hash from the `serverIndices` sorted set to get the virtual server index that the request will be mapped to, and then the corresponding actual server instance is fetched using the `serverList` array. Finally the allocated server name is returned for further processing.  

When a server is removed from the network then the virtual server indexes are cleared from the corresponding data structures.  

### Load Balancer Implementation

#### Global Data Structures used 

`server_names`: This is a list of all the currently running server names.  
`count`: Stores the number of currently running servers.  
`read_request_queue`: Queue for storing all incoming read requests to servers. Similarly, there are queues for each shard for each of the write, update and delete reqeusts.  
`server_name_to_number`: Map from server name to its ID.  
`MapT`: Map from shard id to list of server names which maintain a replica of the shard.  
`ShardT`: Stores details of shards.  

Whenever the `/add` or `/rm` endpoints are called then `server_names` and `count` are updated by using locks appropriately. Queue is threadsafe in Python so there is no need to use locks when appending or popping from  the queues.  

#### Working of the Load Balancer

##### Read request

The load balancer uses a queue to store all the incoming read requests to the `/read` endpoint.  

We use multithreading to service the requests, namely we implement 100 worker threads, each of which does the following:  

   **1.** Pop a request from the front of the request queue. Each request object also contains a response field that will be populated.  
   **2.** Use the previously defined consistent hashing class to find out the server that this request will be allocated to.    
   **3.** Make the request to the server and store the response in the request object's response field.  

The client that makes the request waits on the response field of the request object.  

We have used multithreading in the load balancer to ensure that the allocation of requests to servers and their handling can be done concurrently.  

##### Write, Update, Delete requests

We have implemented a design such that there is a separate thread for each shard for write, update and delete requests.  

We find the shards corresponding to the requests and send them to their respective threads.  

Each thread puts a corresponding shard lock, sends the requests to all the servers containing this shard, and collects and sends the response from the servers.  

##### Handling Failure of containers

We define a heartbeat thread in the load balancer that periodically sends requests to the `/heartbeat` endpoint of all the servers in `server_names`.  
We count the number of responding containers and store the shard information of the down servers if and only if they aren't stopped via `/rm`. The `server_names` and `count` variables are updated accordingly. We then call `/add` for all those servers. We use `/copy` and `/write` to populate the newly created shard replicas in the new server.

### Code Optimizations

We have optimized our code at almost every possible step. Some instances are as follows:

   **1.** In `/read`, we have maintained the complexity to O(number of shards) while searching for shards containing range: low, high. It is the mininmum possible, instead of iterating through complete list of shards.  
   **2.** We have implemented binary search to find the shard which contains the student ID in O(log(N)), where N is the number of shards  
   **3.** We have ensured parallelism at every stage, ensuring read requests are completely parallel, as well as providing separate threads for each shard for write, update and delete requests. Moreover, for write, update and delete requests, we again create separate threads for with a shard-thread while sending requests to different servers.  

## Analysis  

### A-1  

Read time:  
Write time:  

### A-2 

Read time:  
Read speed up:  
Write time:  
Write speed down:  

### A-3

Read time:  
Read speed up:  
Write time:  
Write speed up:  

### A-4  

We have tested all the endpoints of the load balancer including the `/add` and `/rm` endpoints. The heartbeat thread of the load balancer keeps monitoring the server containers and whenever a server is manually dropped, the heartbeat thread itself calls the `/add` endpoint and spawns new containers quickly to handle the load, copying the shard entries from the other replicas at the same time.  
