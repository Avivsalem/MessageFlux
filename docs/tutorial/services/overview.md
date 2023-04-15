# Services

There are several types of services messageflux offer.

The most useful is ```PipelineService``` which can be used to handle messages and send output to other services

A Service is the most basic running unit of messageflux. you create a service and then you start it. 
this will start running the service, and block, until the process receives a termination signal (SIGTERM) 
or until 'stop' was called from another thread.

See reference for customization options for services.



