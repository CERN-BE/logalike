# Logalike
Logalike is a lookalike of [Logstash](https://www.elastic.co/products/logstash), written in Java 8. It was designed and implemented at [CERN](http://home.cern) and is currently used in operations.

---

## Installation
Unfortunately Logalike is not yet available from any  global repository  but we are working on that. However, you can install it manually by pulling the source code in two easy steps:

First, pull the project from GitHub:
``git clone git@github.com:CERN-BE/logalike.git``
Second, fetch the dependencies and build the code. We implemented a gradle script (build.gradle), So if you already have gradle installed, simply run gradle compile.

## Status
This tool is currently under development, though that shouldn't stop you from tying it out and even use it ([CERN](http://home.cern) already does that in production).
[![Build Status](https://travis-ci.org/CERN-BE/logalike.svg?branch=master)](https://travis-ci.org/CERN-BE/logalike)

## Credits
Logalike was written by [Gergő Horányi](https://github.com/ghoranyi) and [Jens E. Pedersen](https://github.com/Jegp) between 2014 and 2016 while working to CERN. Thanks to Vito Baggiolini, [Endre Fejes] (https://github.com/fejese) and [György Demarcsek](https://github.com/dgyuri92r) for invaluable help with everything from general design issues to implementation details.

![CERN logo](http://design-guidelines.web.cern.ch/sites/design-guidelines.web.cern.ch/files/u6/CERN-logo.jpg)
