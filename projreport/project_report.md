## Problem Description
Given the *Order* data and *Trade* data, compute the lowest transaction price level of each recorded market order using HDFS and MapReduce. 

## Task Comprehension
All the market orders are stored in *Order* data, while all the transactions are recorded in the *Trade* data.
Therefore, the core of the task is to use the primary key `ApplSeqNum` to link the two part of data.

## Difficulties Analysis


## Concrete workflow
The concrete workflow of our work is shown below. 

Our work is assigned to two MapReduce jobs. The map phase of the first job (`MAP1` in short) first separates *MarketOrder*, *LimitOrder* and *SpecOrder* from the *Order* data, and *Cancel* and *Traded* from the *Trade* data. Then `MAP1` emits all the data with `ApplSeqNum` as the key (for *Traded*, outputs two `<k,v>` pairs for each traded record with `BidApplSeqNum` and `OfferApplSeqNum` as the key respectively). Afterward the `<k,v>` pairs from the *Traded* with the same `ApplSeqNum` are submitted to `REDUCE1` for counting (which just yields the number `K` of transaction price levels of the market order). In the job 2, `MAP2` takes in the `<ApplSeqNum, K>` pairs and emits them with the `TIMESTAMP` as the key, so that all the records are automatically sorted in the order of time in the shuffle phase before committed to the `REDUCE2` for a final adjustment for the required format.

![](Workflow.svg)

## Code Design