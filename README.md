This readme provides information about dependencies, installation instructions and how to get started with the code.

## Dependencies

Code is Written in Python 2.7+ and Java. Also, it depends on.

* [xLiMe Meta Data Model](https://cloud.aifb.kit.edu/index.php/s/742S1H9ljLlxaSz)
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

##  Installation Instructions (Debian/Ubuntu)

1. `$git clone https://github.com/adityamogadala/xLiMeSemanticIntegrator.git`
2.  Make sure you have python-dev and setuptools. Otherwise install
	* `sudo apt-get install python-dev`
	* `sudo pip install --upgrade setuptools`
3. BLAS/LAPACK are required for scipy and numpy. If not already present, install.
	* `sudo apt-get install libblas-dev liblapack-dev`
2. `$sudo pip install -r requirements.txt`
3. `$sudo pip install kafka-python`
5.  Download Word Embeddings ([Monolingual and Bilingual](http://people.aifb.kit.edu/amo/wordembeddings/)) zip files. Extract and keep them in StoreWordVec/wiki for Wikipedia, StoreWordVec/news for News etc..
6.  Get [MongoDB](https://docs.mongodb.com/v3.0/tutorial/install-mongodb-on-ubuntu/) and run the following. 
	* `$sudo mkdir -p /data/db/` (Create directory at $HOME on disk for MongoDB database)

##  Get Started

* Start MongoDB deamon with authentication and create admin user for all DBs.
	* `$sudo mongod --fork --logpath mongodb.log --auth`
	* `$mongo` 
	* `> use admin`. 
	* `> db.createUser({user:"username",pwd:"password",roles: [{role:"userAdminAnyDatabase",db: "admin"}]}) ` (Create super user and password for the "admin" database).
	* `> exit`
	* `$mongo -u username -p password --authenticationDatabase admin`
	* `> use MyStore` (Create Your own Database which will be used in Config file)
	* `> db.createUser({user:"username",pwd:"password",roles: [{role:"dbOwner",db: "MyStore"}]}) ` (Create Username and Password for the database).
	* `> exit`
* Update config/Config.conf as suggested in the file.
* `$python setup.py`
* Start service/collector.sh to collect data from the Kafka stream. 
	* `$ nohup sh collector.sh &`
* Test if your MongoDB database collections exist and create text indexes for them.
	* `$mongo`
	* `> use MyStore`
	* `> db.auth("username","password")`
	* `> show collections`
	* `> db.YOUR_COLLECTION_NAME.ensureIndex( {Text: "text", Title: "text"}, {dropDups: true} )`
	* `> db.SUBTITLES_COLLECTION.ensureIndex( {Text: "text"}, {dropDups: true} )` (No Titles info for Subtitles.)
	* `> exit`
* Start service/vecgenerator.sh to generate vectors for subtitles and speech to text data (yet to add for news and social media).
	* `$ nohup sh vecgenerator.sh &`
* Examples folder contains few examples on how to use different classes for tasks such as simple search, advanced search, monolingual and cross-lingual document similarity and analytics. You can use individual python files or ipython file (.ipynb) for execution.
