import sys
sys.path.insert(0, '../search')
import SimpleKeywordSearch

xlimerec = SimpleKeywordSearch.XlimeAdvancedRecommender()
#queries = 'From the engineering side, we have also been working on the ability to parallelize training of neural network models over multiple GPU cards simultaneously. We worked on minimizing the parallelization overhead while making it extremely simple for researchers to use the data-parallel and model-parallel modules (that are part of fbcunn). Once the researchers push their model into these easy-to-use containers, the code automatically schedules the model over multiple GPUs to maximize speedup.'
queries = 'GPU cards'
messagelist = xlimerec.recommender(queries)
print messagelist

