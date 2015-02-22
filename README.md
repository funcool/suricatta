# suricatta

[![Travis Badge](https://img.shields.io/travis/niwibe/suricatta.svg?style=flat)](https://travis-ci.org/niwibe/suricatta "Travis Badge")

High level sql toolkit for clojure (backed by jooq library)

## Download

[![Clojars Project](http://clojars.org/suricatta/latest-version.svg)](http://clojars.org/suricatta)

## Quick Start ##

Put suricatta on your dependency list:

```clojure
[suricatta "0.2.1"]
[com.h2database/h2 "1.3.176"] ;; For this example only
```

Define a valid dbspec hashmap:

```clojure
(def dbspec {:subprotocol "h2"
             :subname "mem:"})
```

Connect to the database and execute a query:

```clojure
(require '[suricatta.core :as sc])

(with-open [ctx (sc/context dbspec)]
  (sc/fetch ctx "select x from system_range(1, 2);"))
;; => [{:x 1} {:x 2}]
```

## Documentation ##

- Latest stable documentation: http://niwibe.github.io/suricatta/latest/
- Development documentation: http://niwibe.github.io/suricatta/devel/
