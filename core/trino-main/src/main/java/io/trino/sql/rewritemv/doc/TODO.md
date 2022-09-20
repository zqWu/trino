# where 条件的处理

## 三种类型的 where条件

- PredicateEqual: Ta.C1 = Tb.C2 <==================== Equivalent Class
- PredicateRange: Ta.C1 {> , >= , = , <= , <} 一个常数(字面量 Literal)
- 其他 atomic predicate 条件

| case | query_where | mv_where | extra condition      | process  |
|------|-------------|----------|----------------------|----------|
| 1    | no          | no       |                      | 不用处理     |
| 2    | no          | has      |                      | not fit  |
| 3_1  | has         | no       | mv包含where字段          | 添加 where |
| 3_2  | has         | no       | mv不含where字段          | not fit  |
| 4_1  | has         | has      | mv where更多           | not fit  |
| 4_2  | has         | has      | mv where更少或相等, 且字段不含 | not fit  |
| 4_3  | has         | has      | mv where更少或相等, 且字段包含 | 添加 where |

## EquivalentClass

- where colA=colB and colB=colC === 称 {colA, colB, colC} 为一个 EquivalentClass (等价类)
- 在所有确定字段包含时, 需要考虑到 EquivalentClass
- 例子如下

```sql
-- mv
create
materialized view mv as
SELECT s0, s1, s2
from...
    where s0=s1


-- query
select s0, s1, s2, s3, s4
from...
    where s0=s1
    and s0=s3 and s1=s4
    ...

-- 因为 query有等价类 {s0, s3}, {s1, s4}
-- 在mv 中添加条件 where s0=s3 and s1=s4 后, 需要认为 mv有字段 s3, s4

    select s0, s1, s2, s0 as s3, s1 as s4
from mv
where s0=s3 and s1=s4
```

# groupBy having子句的处理

## groupBy

| case | query_groupBy | mv_groupBy | extra condition     | process          |
|------|---------------|------------|---------------------|------------------|
| 1    | no            | no         |                     | 不用处理             |
| 2    | no            | has        |                     | not fit          |
| 3_1  | has           | no         | mv包含聚合的字段           | 添加 query_groupBy |
| 3_2  | has           | no         | mv不含聚合的字段           | not fit          |
| 4_1  | has           | has        | groupBy 的字段相等       | 去掉 groupBy       |
| 4_2  | has           | has        | mv_groupBy更少        | not fit          |
| 4_3  | has           | has        | mv_groupBy更多, 且字段包含 | 添加 query_groupBy |
| 4_4  | has           | has        | mv_groupBy更多, 且字段不含 | not fit          |

## having处理

- having仅在 groupBy后
- 添加 having/where 的含义
    - 优先使用 where条件
    - 当where条件不满足, 且包含groupBy子句时, 用having子句

| case | query_having | mv_having | extra condition | process         |
|------|--------------|-----------|-----------------|-----------------|
| 1    | no           | no        |                 | 不用处理            |
| 2    | no           | has       |                 | not fit         |
| 3_1  | has          | no        | mv包含having的字段   | 添加 having/where |
| 3_2  | has          | no        | mv不含having的字段   | not fit         |
| 4_1  | has          | has       | having 的相等      | 去掉 having       |
| 4_2  | has          | has       | mv_having更少     | 添加 having/where |
| 4_3  | has          | has       | mv_groupBy更多    | not fit         |

# join的处理

# with子句

# 函数

# 重构为 AstVisitor

