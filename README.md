Simple map reduce algorithm implemented to filter a json coming from Lifesum API.
The json has about ~500 million entries, and should be parsed by 300 chunks of data.
The API limit of max 5 reqs/sec is being respected with the help of a custom semaphore,
that not only controls how many operations can be done in parallel, it is also based
on a time range - in this case is 1sec[1].

First install the dependencies:

    $ make deps

If you wish to run its tests:

    $ make tests

To run it:

    $ make run

The output is kind of noisy, I'm working on improve that soon.


Performance
-----------

The algorithm bottleneck is the network, it takes about 2ms to 5ms to map and reduce 300 entries from
the JSON, but as we're not allowed to do more than 5 reqs/sec to the API, the resource consumption
becomes the bottleneck. Also, from Brazil the time to perform a request on the API is 1s to 2s, it rarely
takes less than one second to perform the request, so the algorithm feels really slow because of the
network :(

If each request takes one second to complete, it would take us about 20 days to process all the data.
If is possible to process 5 reqs/sec, the algorithm would take about 4 days to complete.
