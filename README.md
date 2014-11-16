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

Number of objects to retrieve: 5000

    $ time make run
    [...] # omitted output
    real    0m40.967s
    user    0m2.530s
    sys     0m1.756s

Number of objects to retrieve: 50000

    $ time make run
    [...] # omitted output
    real    2m24.529s
    user    0m42.537s
    sys     0m36.840s


Number of objects to retrieve: 5000000
    $ time make run
    [...] # omitted output
