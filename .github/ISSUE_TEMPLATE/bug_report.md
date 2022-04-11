---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

**Describe the bug**

> A clear and concise description of what the bug is.

**Kafka Version**

> * What version(s) of Kafka are you testing against?
> * What version of kafka-go are you using?

**To Reproduce**

> Resources to reproduce the behavior:

```yaml
---
# docker-compose.yaml
#
# Adding a docker-compose file will help the maintainers setup the environment
# to reproduce the issue.
#
# If one the docker-compose files available in the repository may be used,
# mentioning it is also a useful alternative.
...
```

```go
package main

import (
    "github.com/segmentio/kafka-go"
)

func main() {
    // Adding a fully reproducible example will help maintainers provide
    // assistance to debug the issues.
    ...
}
```

**Expected Behavior**

> A clear and concise description of what you expected to happen.

**Observed Behavior**

> A clear and concise description of the behavior you observed.

```
Often times, pasting the logging output from a kafka.Reader or kafka.Writer will
provide useful details to help maintainers investigate the issue and provide a
fix. If possible, providing stack traces or CPU/memory profiles may also contain
valuable information to understand the conditions that triggered the issue.
```

**Additional Context**

> Add any other context about the problem here.
