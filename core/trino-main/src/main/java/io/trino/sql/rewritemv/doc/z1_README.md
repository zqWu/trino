# goal 01: 单表 mv的完整替换


# 遇到的一些问题以及对应解决
1. 不能在 SqlQueryExecution中处理 analyze, analyze以后会得到一些信息保存到session, 如果替换了 Analysis则无法获取到之前分析得到的数据
```
private final Analysis originalAnalysis;
private final Analysis analysis;

.... // SqlQueryExecution 构造函数中
// this.analysis = analyze(preparedQuery, stateMachine, warningCollector, analyzerFactory);
this.originalAnalysis = analyze(preparedQuery, stateMachine, warningCollector, analyzerFactory);
this.analysis = mvRewrite();

...
private Analysis mvRewrite() {
    return MaterializedViewRewriter.rewrite(getSession(), originalAnalysis);
}
```
解决: 只能在 SqlQueryExecution之前进行 mv rewrite, 因此放到 rewrite阶段
可以自己analyze 原statement, 进行处理

# 代码统计
```bash
cd ~/ws/trino/core/trino-main/src/main/java/io/trino/sql/rewritemv
find . -type f -name "*.java" | xargs wc -l | awk '{print $1}' | datamash sum 1
```
