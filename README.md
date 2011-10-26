# clojalk [![Build Status](https://secure.travis-ci.org/sunng87/clojalk.png)](http://travis-ci.org/) #

A distributed task queue written pure in clojure. A Beanstalkd clone.

## Usage ##

### Installation ###

Clojalk is still in development so we don't have a packaged release. 
To use clojalk, you should checkout the code base and build
it by yourself. This is not difficult task but be sure you have
leiningen installed.

    git clone git@github.com:sunng87/clojalk.git
    cd clojalk
    lein uberjar clojalk.jar

To start a clojalk server:

    java -jar clojalk.jar [clojalk.properties]

Clojalk will load a property file "clojalk.properties" from current
working directory if you don't specify a custom file path from command
line.

Also you can start clojalk from code base with lein. This is only for
test purpose:

    lein run

Try out your installation:

    telnet 127.0.0.1 12026

You should be familiar with beanstalkd's memcached-styled protocol.
 
### Protocol ###

Clojalk is almost fully compatible with Beanstalkd's protocol. So you
can refer to the [protocol
document](https://github.com/kr/beanstalkd/blob/master/doc/protocol.txt
"Beanstalkd Protocol") of Beanstalkd which also works with clojalk.

Commands supported by clojalk are listed here.

Tube operations:

* watch
* use
* ignore
* pause-tube

Job life-cycle operations:

* put
* reserve
* reserve-with-timeout
* delete
* release
* bury
* kick
* touch

Monitoring commands:

* stats
* stats-job
* stats-tube
* list-tubes-watched
* list-tube-used
* list-tubes
* peek
* peek-ready
* peek-delayed
* peek-buried

### Clients ###

The clojure client [beanstalk](https://github.com/sunng87/beanstalk
"beanstalk") is forked and maintained by me, which works with clojalk
and beanstalkd.

More clients to be tested against clojalk.

## Thanks ##

I should thanks [Keith Rarick](https://github.com/kr "Keith Rarick")
who designed beanstalkd and its protocol.

And also I received great help from [Zach
Tellman](https://github.com/ztellman "Zach Tellman") on implementing
the protocol with gloss.

### Contributors ###

* [xiaonaitong](https://github.com/xiaonaitong "xiaonaitong")

## License ###

Copyright (C) 2011 [Sun Ning](http://sunng.info/ "Sun Ning")

Distributed under the Eclipse Public License, the same as Clojure uses. 

