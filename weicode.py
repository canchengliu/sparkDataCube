"""


lgorithm 2. Overall MR-Cube Algorithm MR-CUBE(Cube Lattice C, Data set D, Measure M)
1 Dsample 1⁄4 SAMPLEðDÞ
2 RegionSizes R 1⁄4 ESTIMATE-MAPREDUCE
ðDsample ; CÞ
3 Ca 1⁄4 ANNOTATEðR; CÞ # value part. & batching 4 while (D)
5 do R
6 D
7 Ca
8 Result
9 return Result
R [ MR-CUBE-MAPREDUCEðCa ; M; DÞ
D’ # retry failed groups D’ from MR-Cube-Reduce INCREASE-PARTITIONINGðCa Þ
MERGE(R)# post-aggregate value partitions
Algorithm 3. MR-Cube Phase 1: Annotation MapReduce ESTIMATE-Map(e)
1 #eisatupleinthedata
2 let C be the Cube Lattice;
3 foreachci inC
4 do EMITðci;ciðeÞ ) 1Þ # the group is the secondary key
ESTIMATE-REDUCE/COMBINE(hr; gi; fe1; e2; . . .g) 1 # hr; gi are the primary/secondary keys
2 MaxSizeS fg
3 for each r,g
4 do S[r] MAX(S[r],jgj)
5 # jgj is the number of tuples fei;...;ejg 2 g 6 return S

Algorithm PipeSort

function PS_MAP(e):
	# e is a tuple in the dataset
	C = getPipeline() // C is a list of pipeline, pipeline is a list of group with the same 前缀
	batch_id = 0
	for all R in C do:
		re = R(e) // R(e): extra the 属性 of longest group in R from e
		do EMIT(batch_id, re=>uid) // uid: record id, the uid of tuple e
		batch_id += 1

function PS_REDUCE(batch_id, [kv1, kv2, kv3....]):
	


"""