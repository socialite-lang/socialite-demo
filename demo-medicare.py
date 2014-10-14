import time
import sys

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

print "Reading hcpcs-code.txt"
# hcpcs_code.txt: cpt-code id <tab> description
# Specialty table maps specialty id to its description.
`Code(int code:0..$MAX_HCPCS_CODE_ID, String descr).
 Code(code, descr) :- l=$read("data/hcpcs-code.txt"), 
                           (_, descr, _code) = $split(l, "\t"),
                           code = $toInt(_code).`

def pickSrc():
    #for s in [374409, 731884, 429817, 307369, 182465, 439422, 553886, 559380, 384141, 146989]:
    for s in [136632, 325275, 366319, 379090, 481917, 254858, 448461, 815125, 496378, 391998]:
        yield s

def REAL_pickSrc():
    import random
    while True:
        npi = random.randint(0, MAX_NPI_ID)

        _, specialty, _, _ = `Doctor($npi, specialty, _, _)`.next()
        total=0; same=0
        for _, npi2 in `DocNet($npi, npi2)`:
            total+=1
            _, s2, _, _ = `Doctor($npi2, specialty, _, _)`.next()
            if specialty==s2:
                same+=1
        if total > 20 and float(same)/total >= 0.79:
            yield npi

src = REAL_pickSrc()
for SRC in src:
    # Retrieving the specialty of the SRC
    _, specialty, _, _ = `Doctor($SRC, specialty, _, _)`.next()

    # Retrieving the description of the specialty 
    _, specialty_descr = `Specialty($specialty, descr)`.next()
    print "Source node: %d(%s)"%(SRC, specialty_descr)

    print "Running Personalized PageRank..."

    # Rank table stores PageRank values for doctors.
    # (doctor-id, iteration #, rank value)
    # The table only stores the values for the recent two iterations.
    `Rank(int npi:0..$MAX_NPI_ID, int i:iter, float rank) groupby(2).`
    sys.stdout.write("..")

    # Init PageRank values
    # For the neighbor nodes of the SRC, we initialize the PageRank value of the nodes to be 1/degree
    `Rank(npi, 0, r) :- DocNet($SRC, npi), EdgeCnt($SRC, cnt),  r = 1.0f/cnt.`
    for i in range(20):
        # The first body is jump to the neighbor nodes of the source node (with probabality 0.2)
        # The second body is random walk from one node to its neighbor nodes.
        `Rank(n, $i+1, $sum(r)) :- DocNet($SRC, n), EdgeCnt($SRC, cnt), r=0.2f*1.0f/cnt; 
                                :- Rank(s, $i, r1), EdgeCnt(s, cnt), r = 0.8f*r1/cnt, r>0.0000001, DocNet(s, n).`
        sys.stdout.write("..")
        sys.stdout.flush()
    print
    # DocByRank stores the doctors (NPI) sorted by their PageRank value (descending order)
    `MaxRank(int i:0..0, float rank) groupby(1).
     MaxRank(0, $max(rank)) :- Rank(npi, $i, rank).`
    _, maxRank = `MaxRank(0, rank)`.next()
    threshold = maxRank*0.001

    `DocByRank(int i:0..0, (int npi:4096, float rank)) sortby rank desc.
     DocByRank(0, npi, rank) :- Rank(npi, $i, rank), rank>$threshold.`

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


    print "+------------------------------------------+"
    print "Cluster of ", specialty_descr
    print "  Source NPI:", SRC


    print "  Cluster size:", len(cluster)
    print "  Specialty distributions:"
    for specialty, cnt in specialtyCount.items():
        _, descr = `Specialty($specialty, descr)`.next()
        print "    %s:%d"%(descr, cnt)

    clusterSpecialty = None
    for k,v in specialtyCount.items():
        if v==majority:
            clusterSpecialty = k

    # we print a couple of doctors having specialty that is different from the rest doctors in the cluster
    print "  Anomaly Candidates:"
    count=0
    for npi, rank in cluster:
        _, specialty, _, _ = `Doctor($npi, specialty, _, _)`.next()
        if specialty == clusterSpecialty:
            continue

        print "  %d.NPI:%s Rank:%f"%(count+1, npi, rank)
        _, descr = `Specialty($specialty, descr)`.next()
        print "    Specialty:", descr
        for _, specialty, code, freq in `Doctor($npi, specialty, code, freq)`:
            _, descr = `Code($code, descr)`.next()
            print "\t  ", descr, ":", freq

        count+=1
        if count==5: break

    print "-----------------------------\n"

    `clear Rank.
     clear DocByRank.`

sys.exit(0)
