import seaborn as sns
from icecream import ic 
import statistics
import matplotlib.pyplot as plt
def report(title, l):
    if len(l) != 10:
        ic("ERRRORORORORORO")
    ic(title)
    ic(statistics.mean(l))
    ic(statistics.stdev(l))
    ic(statistics.stdev(l)/statistics.mean(l))

rows1_cache = [ 02.327453 ,02.244904 ,02.243381 ,02.239009 ,02.325356 ,02.28227 ,02.283176 ,02.243427 ,02.24705 ,02.352852]
rows10_cache = [02.413193, 02.246193, 02.277583, 02.298591, 02.288751, 02.246741, 02.344774, 02.24673, 02.32363, 02.294387,]
rows100_cache=[2.407814, 02.302662, 02.304119, 02.292154, 02.275054, 02.341135, 02.261017, 02.335915, 02.291913, 02.317959,]
rows1_000_cache=[02.441226, 02.369224, 02.395233, 02.34214, 02.408794, 02.495894, 02.341042, 02.394548, 02.319347, 02.573989,]
rows10_000_cache = [03.090466, 03.158055, 03.516805, 03.151766, 03.150267, 03.136304, 03.215509, 03.152601, 03.223561, 03.263487]
rows100_000_cache=[18.145742, 13.788799, 18.057874, 13.737922, 18.396022, 13.828113, 18.103763, 13.598867, 18.074779, 13.78361, ]
rows1_000_000_cache =[ 148.565327 , 148.319243 , 149.055756 , 146.267693 , 148.962543 , 148.440448 , 147.065989 , 150.151061 , 147.217101 , 149.762996 , ]
rows10_000_000_cache=[4516.123203, 4509.913308, 4520.493947, 4510.862754, 4518.501272, 4534.262348, 4508.87906, 4508.968458, 4499.470584, 4486.59612,]


rows1_nocache=[18.601258, 20.074349, 18.337346, 19.187702, 19.455436, 19.022672, 19.327099, 19.522442, 18.784998, 19.587174,]
rows10_nocache=[ 19.910499, 20.0065, 18.543487, 20.053662, 18.969858, 19.267897, 20.747899, 19.050508, 20.562188, 19.293296,]
rows100_nocache=[ 19.321052, 19.958123, 19.062314, 19.181107, 20.056709, 18.966955, 19.446096, 19.038991, 18.6752, 20.03636,]
rows1_000_nocache=[19.43901, 19.70756, 19.427013, 19.050441, 19.782597, 19.11958, 18.814945, 19.935515, 19.263157, 20.271337,]
rows10_000_nocache = [19.868025, 19.955663, 21.396638, 19.960547, 21.165915, 20.579483, 19.593327, 20.867357, 19.478206,20.748564,]
rows100_000_nocache = [ 49.987036, 47.801841, 47.177201, 50.187212, 48.896773, 46.938819, 47.220516, 49.393727, 49.137539, 49.493073,]
rows1_000_000_nocache=[337.101481, 335.402569, 356.295999, 326.175175, 360.835231, 345.047076, 363.464622, 347.404392, 341.221677, 355.316935,]
rows10_000_000_nocache=[ 6601.525244, 6500.82657, 6449.635076, 6477.569025, 6532.822508, 6574.818591, 6453.825265, 6445.749903, 6518.228769, 6488.11197]


#report('       1 rows, cache', rows1_cache)
#report('      10 rows, cache', rows10_cache)
#report('     100 rows, cache', rows100_cache)
#report('    1000 rows, cache', rows1_000_cache)
#report('   10000 rows, cache', rows10_000_cache)
#report('  100000 rows, cache', rows100_000_cache)
#report(' 1000000 rows, cache', rows1_000_000_cache)
#report('10000000 rows, cache', rows10_000_000_cache)
#report('       1 rows, nocache', rows1_nocache)
#report('      10 rows, nocache', rows10_nocache)
#report('     100 rows, nocache', rows100_nocache)
#report('    1000 rows, nocache', rows1_000_nocache)
#report('   10000 rows, nocache', rows10_000_nocache)
#report('  100000 rows, nocache', rows100_000_nocache)
#report(' 1000000 rows, nocache', rows1_000_000_nocache)
#report('10000000 rows, nocache', rows10_000_000_nocache)

means_nocache = [statistics.mean(l) for l in [rows1_nocache,
                                              rows10_nocache ,
                                              rows100_nocache ,
                                              rows1_000_nocache ,
                                              rows10_000_nocache ,
                                              rows100_000_nocache,
                                              rows1_000_000_nocache ,
                                              rows10_000_000_nocache]]
means_cache = [statistics.mean(l) for l in [rows1_cache,
                                            rows10_cache ,
                                            rows100_cache ,
                                            rows1_000_cache ,
                                            rows10_000_cache ,
                                            rows100_000_cache ,
                                            rows1_000_000_cache ,
                                            rows10_000_000_cache]]
rows= [1,
       10,
       100,
       1000,
       10000,
       100000,
       1000000,
       10000000]

print(len(means_nocache))
print(len(means_cache))
print(len(rows))

data = {
        'x': rows+rows,
        'y':means_cache+means_nocache,
        'uso caché': ['sí'] * len(rows) + ['no']* len(rows)
        }
print(data)

plt.figure(figsize=(7, 5))
sns.barplot(data=data, x='x', y='y', hue='uso caché')
#plt.xscale('log')
plt.yscale('log')
plt.title('')
plt.xlabel('Número de filas leídas')
plt.ylabel('Tiempo(ms)')
#plt.show()
#plt.savefig('sns1.png', dpi=200, bbox_inches='tight')

#ic(means_cache)

###########
print(statistics.mean([5370.171593, 4348.891503, 4033.927002, 4831.996519, 4081.330098, 3990.538053, 4066.131099, 4006.339857, 3996.168569, 4073.180145,]))
