import time

MAX_NPI_ID = 880643
MAX_HCPCS_CODE_ID = 5948

print "Reading doc-net.txt"

# doc-net.txt: doctor-id1 <tab> doctor-id2
# (there's an edge between doctor-id1 and doctor-id2 if they are similar in terms of their prescriptions)
# DocNet table stores the edges (source id, target id).
# EdgeCnt table stores the number of neighbor nodes (node id, neighbor count)
`DocNet(int npi:0..$MAX_NPI_ID, (int npi2:128)) multiset.
 DocNet(npi1, npi2) :- l=$read("data/doc-net.txt"), 
                       (_npi1, _npi2)=$split(l, "\t"),
                        npi1=$toInt(_npi1),
                        npi2=$toInt(_npi2).

 DocNet(npi2, npi1) :- DocNet(npi1, npi2). 
 
 EdgeCnt(int npi:0..$MAX_NPI_ID, int cnt).
 EdgeCnt(npi, $inc(1)) :- DocNet(npi, npi2). `

print "Reading npi-cpt-code.txt"

# npi-cpt-code.txt: doctor id <tab> doctor specialty <tab> cpt code <tab> claim count for the cpt code
# Doctor table stores the information (doctor, specialty, cpt code, count)
`Doctor(int npi:0..$MAX_NPI_ID, int specialty, (int code, int freq)).
 Doctor(npi, specialty, code, freq) :- l=$read("data/npi-cpt-code.txt"), 
                                       (_npi, _spec, _code, _freq) = $split(l, "\t"),
                                       npi = $toInt(_npi),
                                       specialty = $toInt(_spec),
                                       code = $toInt(_code),
                                       freq = $toInt(_freq).`

MAX_SPECIALTY_ID = 88

print "Reading specialty.txt"
# specialty.txt:  specialty-id <tab> description
# Specialty table maps specialty id to its description.
`Specialty(int specialty:0..$MAX_SPECIALTY_ID, String descr).
 Specialty(spec, descr) :- l=$read("data/specialty.txt"), 
                           (descr, _spec) = $split(l, "\t"),
                           spec=$toInt(_spec).`

print "Reading hcpcs_code.txt"
# hcpcs_code.txt: cpt-code id <tab> description
# Specialty table maps specialty id to its description.
`Code(int code:0..$MAX_HCPCS_CODE_ID, String descr).
 Code(code, descr) :- l=$read("data/hcpcs_code.txt"), 
                           (_, descr, _code) = $split(l, "\t"),
                           code = $toInt(_code).`

print "Running Personalized PageRank algorithm on the similarity graph of doctors"

SRC=7

_, specialty, _, _ = `Doctor($SRC, specialty, _, _)`.next()
_, specialty_descr = `Specialty($specialty, descr)`.next()
print "Source doctor:", SRC, "with specialty:", specialty_descr

# Rank table stores PageRank values for doctors.
# (doctor-id, iteration #, rank value)
# The table only stores the values for the recent two iterations.
`Rank(int npi:0..$MAX_NPI_ID, int i:iter, double rank) groupby(2).`

# Init PageRank values
`Rank(npi, 0, r) :- DocNet($SRC, npi), EdgeCnt($SRC, cnt),  r = 1.0/cnt.`
for i in range(50):
    # The first body is jump to the neighbor nodes of the source node (with probabality 0.2)
    # The second body is random walk from one node to its neighbor nodes.
    `Rank(n, $i+1, $sum(r)) :- DocNet($SRC, n), EdgeCnt($SRC, cnt), r=0.2*1.0/cnt; 
                            :- Rank(s, $i, r1), EdgeCnt(s, cnt), DocNet(s, n), r = 0.8*r1/cnt.`

# DocByRank stores the doctors (NPI) sorted by their PageRank value (descending order)
`DocByRank(int i:0..0, (int npi:1024, double rank)) sortby rank desc.`
 DocByRank(0, npi, rank) :- Rank(npi, $i, rank).`


specialtyCount={}
cluster=[]
# We traverse the doctors in the descending order of their PageRank value
# and add the doctors to the cluster.
for _, npi, rank in `DocByRank(0, npi, rank)`:
    cluster.append((npi, rank))
    _, specialty, _, _ = `Doctor($npi, specialty, _, _)`.next()
    try: specialtyCount[specialty] = specialtyCount[specialty]+1
    except: specialtyCount[specialty] = 1

    # we wait until the cluster size > 30
    if len(cluster)<30: continue
    # we keep adding the doctors as long as they have the same specialty
    if len(specialtyCount) == 1: continue

    # we wait until there are a few different specialty in the cluster
    majority=0
    sum=0
    for k,v in specialtyCount.items():
        if v>majority: majority = v
        sum+=v
    if float(majority)/sum > 0.9: continue
    if sum-majority > 10:break


print "Cluster size:", len(cluster)
print "-----------------------------"
for specialty, cnt in specialtyCount.items():
    _, descr = `Specialty($specialty, descr)`.next()
    print descr, ":", cnt
print "-----------------------------"

clusterSpecialty = None
for k,v in specialtyCount.items():
    if v==majority:
        clusterSpecialty = k

# we print a couple of doctors having specialty that is different from the rest doctors in the cluster
print "Anomalies:"
count=0
for npi, rank in cluster:
    _, specialty, _, _ = `Doctor($npi, specialty, _, _)`.next()
    if specialty == clusterSpecialty:
        continue

    print "\tNPI:", npi, "Rank:", rank
    _, descr = `Specialty($specialty, descr)`.next()
    print "\tSpecialty:", descr
    for _, specialty, code, freq in `Doctor($npi, specialty, code, freq)`:
        _, descr = `Code($code, descr)`.next()
        print "\t  ", descr, ":", freq

    count+=1
    if count==5: break

print "-----------------------------\n"
