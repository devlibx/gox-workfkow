

### Config
Give below is the sample configuration file for the worker groups. The configuration file is in 
yaml format. It allows you to define multiple worker groups - and you can work with more than one 
cadence server

A typical configuration file will look like below
```yaml
worker_groups:
  worker_group:
    domain: your_domain
    host_port: localhost:7933
    name: server_1
    worker:
    - task_list: server_1_ts_1
      worker_count: 3
    - task_list: server_1_ts_2
      worker_count: 3
```

Suppose you want to work with more than one cadence server, you can define multiple worker groups. If this 
is the case then the only limitation is you should have unique task list names across all the worker groups.
```yaml
worker_groups:
  worker_group_1:
    domain: your_domain
    host_port: localhost:7933
    name: server_1
    worker:
    - task_list: server_1_ts_1
      worker_count: 3
    - task_list: server_1_ts_2
      worker_count: 3
  worker_group_2:
    domain: your_domain_2
    host_port: localhost:7933
    name: server_2
    worker:
    - task_list: server_2_ts_1
      worker_count: 3
    - task_list: server_2_ts_2
      worker_count: 3

```