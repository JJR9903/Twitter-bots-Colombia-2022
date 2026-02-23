# Twitter-bots-Colombia-2022

Identifying Twitter bots with Machine Learning in presidential elections
-- Colombia 2022

This repository contains code and notebooks used to collect, label,
analyze, and model Twitter users from the 2022 Colombian presidential
election period for the purpose of detecting automated accounts ("bots")
using machine learning.

------------------------------------------------------------------------

## 📌 Project Overview

After the 2022 presidential elections in Colombia, we used the Twitter
API to gather data from users actively tweeting about election debates
and political discussions.

Users were labeled using Botometer, and we engineered multiple features
from tweet text and account metadata. A Random Forest classifier was
trained to distinguish between human and bot accounts.

Feature categories include:

-   Tweet sentiment and emoji-based NLP features
-   Number of tweets, retweets, followers, and followings
-   Time-based interaction metrics
-   Engagement with Colombian politicians
-   Mean time between tweets
-   Additional behavioral and metadata-based variables

------------------------------------------------------------------------

## 📁 Repository Structure

    ├── Bot S.py
    ├── Bot V.py
    ├── Botometer Marcas.py
    ├── Consolidated_Vale.ipynb
    ├── Entrega 0 (Descriptivas usuarios).ipynb
    ├── Random Forest.ipynb
    ├── Sentimientos.ipynb
    ├── Timelines.py
    ├── user_model_Profile_Caracteristics.py
    ├── .gitignore
    ├── LICENSE
    └── README.md

Most of the analysis and modeling workflow is implemented in Jupyter
Notebooks (.ipynb), supported by Python scripts for data collection and
preprocessing.

------------------------------------------------------------------------

## 🧠 Core Components

### 🐦 Data Collection

Scripts leveraging the Twitter API to retrieve user timelines and tweet
data related to the Colombian 2022 elections.

### 🤖 Bot Labeling

Integration with Botometer to score Twitter accounts and generate
supervised learning labels.

### 📊 Feature Engineering

Construction of behavioral, temporal, and NLP-based features for each
user.

### 🧪 Machine Learning

Training and evaluation of a Random Forest classifier to predict bot
vs. human accounts.

------------------------------------------------------------------------

## 📦 Packages Used

The project was developed in Python using the following main libraries:

-   tweepy -- Twitter API access
-   botometer -- Bot detection scoring
-   pandas -- Data manipulation
-   numpy -- Numerical computation
-   scikit-learn -- Machine learning models (Random Forest)
-   matplotlib -- Data visualization
-   seaborn -- Statistical visualization
-   nltk -- Natural language processing
-   textblob -- Sentiment analysis
-   emoji -- Emoji extraction and processing
-   tqdm -- Progress tracking
-   jupyter notebook -- Analysis environment

------------------------------------------------------------------------

## 📜 License

This project is released under the MIT License.

------------------------------------------------------------------------

## 🙌 Contributors

-   Valentina Castilla
-   Santiago Herrera
-   Juan Diego Heredia
-   Juan José Rincón
