beats-output-http
=================

Outputter for the Elastic Beats platform that simply
POSTs events to an HTTP endpoint.

[![Build Status](https://travis-ci.org/raboof/beats-output-http.svg?branch=master)](https://travis-ci.org/raboof/beats-output-http)

Usage
=====

To add support for this output plugin to a beat, you
have to import this plugin into your main beats package,
like this:

```
package main

import (
	"os"

	_ "filebeat/outputs/http"

	"github.com/elastic/beats/filebeat/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

```

Then configure the http output plugin in filebeat.yaml:

```
output:
  http:
    hosts: ["some.example.com:80/foo"]
```
