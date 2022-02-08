# springboot-restHighLevelClient
### 说明
本工程是一个封装了restHighLevelClient的场景工具

### 开放自定义配置的properties
1. boolean enable 是否启用，默认为false，只有配置为true后才能生效  
2. 启用后，可以配置ElasticSearch部署的机器的host、port和scheme，scheme默认为http

### 使用说明
1. 配置好enable、host和post后，会自动创建ElasticSearchUtils类对象
2. 通过该对象可以对es进行索引和文档的增删查改、文档的复杂搜索、聚合搜索和聚合搜索结果解析等
