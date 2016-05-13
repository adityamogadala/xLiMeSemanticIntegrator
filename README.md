This readme provides information about dependencies, installation instructions and how to get started with the code.

## Dependencies

Code is Written in Python 2.7+ and Java. Also, it depends on.

* [Numpy](http://www.numpy.org/)
* [Scipy](https://www.scipy.org/install.html)
* [sklearn](http://kafka.apache.org/)
* [gensim](https://radimrehurek.com/gensim/)
* [LangID](https://github.com/saffsd/langid.py)
* [Rake] (https://github.com/aneesha/RAKE)
* [Pymongo-2.8] (https://pypi.python.org/pypi/pymongo/2.8)
* [Kafka Python Client] (https://github.com/dpkp/kafka-python)
* [MongoDB](https://www.mongodb.com/)
* [Apache Kafka](http://kafka.apache.org/)

##  Installation Instructions

1. `$git clone https://github.com/adityamogadala/xLiMeSemanticIntegrator.git`
2. `$pip install -r requirements.txt`
3. `$pip install kafka-python`
3.  Download Word Embeddings ([Monolingual and Bilingual](http://people.aifb.kit.edu/amo/wordembeddings/)) zip files. Extract and keep them in StoreWordVec/wiki for Wikipedia, StoreWordVec/news for News etc..
4.  Get [MongoDB](https://www.mongodb.com/) and run the following. 
	* `$cd MongoDBfolder`
        * `$mkdir /data/db/` (Create local directory on disk for MongoDB database)
	* `$./mongo` 
	* `$use MyStore` (Creates a MongoDB database "MyStore"). 
	* `$db.addUser("username","password") `  (Creates Username and Password for the database to secure it).

##  Get Started

* Update config/Config.conf as suggested in the file.
* Start MongoDB deamon with authentication.
	* `$./mongod --dbpath ../data/db --fork --logpath mongodb.log --auth`
* Start service/collector.sh to collect data from the Kafka stream. 
	* `$ nohup sh collector.sh &`
* Examples folder contains few examples on how to use different classes for tasks such as simple search, advanced search, monolingual and cross-lingual document similarity and analytics. You can use individual python files or ipython file (.ipynb) for execution.
