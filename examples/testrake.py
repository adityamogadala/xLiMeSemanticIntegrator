import rake
query = "From the engineering side, we've also been working on the ability to parallelize training of neural network models over multiple GPU cards simultaneously. We worked on minimizing the parallelization overhead while making it extremely simple for researchers to use the data-parallel and model-parallel modules (that are part of fbcunn). Once the researchers push their model into these easy-to-use containers, the code automatically schedules the model over multiple GPUs to maximize speedup."
rake1 = rake.Rake("SmartStoplist.txt")
vals = rake1.run(query)
print vals[0][0], vals[1][0], vals[2][0]
