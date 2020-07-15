# XEvent-Capture
A high throughput extended event capture system for distributed Microsoft SQL Servers, capable of reading multiple event streams and outputting to one central monitoring location.

In its simplest form, just enter a server name, pick the events to capture, set the session to active and the application will handle the rest.  The default xevent session has been tested to have minimal impact on server performance and will capture all waits, blocking, deadlocks, rowset calls with ability to filter at application level, removing load on event sessions on target servers.

Event dataloss dependent on the size of your boat ;)


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* .NET 4.7.2
* Microsoft SQL Server 2014
* Visual Studio 2017


### Installing

TBC

```
TBC
```

## Built With

* [VS2017](https://www.visualstudio.com/)
* [XEvent](https://msdn.microsoft.com/en-us/library/bb630282(v=sql.105).aspx) - EventStream


## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

For the versions available, see the [tags on this repository](https://github.com/callsopp/XEvent-Capture/tags). 

## Authors

* **Chris Allsopp** - *Initial work* - [Callsopp](https://github.com/callsopp)

See also the list of [contributors](https://github.com/callsopp/XEvent-Capture/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* No thanks to Microsoft for making life so difficult ;)
* Bye bye profiler - hello extended events!
