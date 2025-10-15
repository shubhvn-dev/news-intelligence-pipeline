# 📰 News Intelligence Pipeline

A production-ready data engineering pipeline that ingests, processes, and analyzes global news events using Apache Spark and AI-powered summarization with Google Gemini.

![Dashboard Preview](docs/dashboard_preview.png)

## 🎯 Project Overview

This project demonstrates a complete end-to-end data pipeline that:
- Ingests real-time global news data from GDELT (1,800+ events)
- Processes data using Apache Spark for distributed computing
- Enriches events with AI-generated summaries using Google Gemini 2.0
- Visualizes insights through an interactive Streamlit dashboard

**Live Demo:** [Coming Soon - Streamlit Cloud]

## 🏗️ Architecture
```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   GDELT     │────▶│   Bronze     │────▶│   Spark     │────▶│   Silver     │
│  Data API   │     │   Layer      │     │ Processing  │     │   Layer      │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
                                                                       │
                                                                       ▼
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│  Streamlit  │◀────│     Gold     │◀────│   Gemini    │◀────│   Silver     │
│  Dashboard  │     │    Layer     │     │ AI Summary  │     │   Layer      │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
```

**Data Layers:**
- **Bronze:** Raw GDELT data (1,904 events)
- **Silver:** Cleaned & validated data (1,835 events, 96.4% quality)
- **Gold:** AI-enriched with summaries (1,835 events with Gemini summaries)

## ✨ Features

### 📊 Data Pipeline
- **Real-time ingestion** from GDELT Project (updates every 15 minutes)
- **Distributed processing** with Apache Spark
- **Data quality validation** with 96.4% retention rate
- **Medallion architecture** (Bronze → Silver → Gold layers)

### 🤖 AI Integration
- **Google Gemini 2.0 Flash** for intelligent summarization
- **Article fetching** from source URLs
- **Rich context** including dates, locations, actors
- **Cost optimization** (~$0.07 for 1,835 events)

### 📈 Interactive Dashboard
- **Geographic visualization** with interactive maps
- **Sentiment analysis** across countries and actors
- **Advanced search** with multiple filters
- **Data export** (CSV, JSON, TXT)
- **Real-time analytics** powered by DuckDB

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- 8GB RAM minimum
- Google Gemini API key ([Get one free](https://makersuite.google.com/app/apikey))

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/news-intelligence-pipeline.git
cd news-intelligence-pipeline
```

2. **Set up environment variables**
```bash
# Create .env file
cp .env.example .env

# Add your Gemini API key
echo "GEMINI_API_KEY=your_api_key_here" >> .env
```

3. **Start the infrastructure**
```bash
# Start all services
docker-compose up -d

# Wait for services to be ready (~60 seconds)
```

4. **Access the services**
- **Airflow UI:** http://localhost:8082 (user: `airflow`, password: `airflow`)
- **Spark Master:** http://localhost:8080
- **Dashboard:** Run `streamlit run streamlit/app.py`

### Running the Pipeline

1. **Ingest Data** (Airflow UI)
   - Navigate to http://localhost:8082
   - Trigger `gdelt_ingestion` DAG
   - Wait ~2 minutes for completion

2. **Process with Spark**
```bash
   docker-compose exec spark-master /opt/spark/bin/spark-submit \
     --master local[*] \
     /opt/spark-jobs/gdelt_processor.py \
     /opt/spark-data/bronze/gdelt \
     /opt/spark-data/silver/gdelt \
     2025-10-15
```

3. **AI Enrichment** (Airflow UI)
   - Trigger `gemini_processing` DAG
   - Processing time: ~2.5 hours for 1,835 events
   - Cost: ~$0.07

4. **Launch Dashboard**
```bash
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install streamlit plotly duckdb pandas
   
   # Run dashboard
   streamlit run streamlit/app.py
```

## 📁 Project Structure
```
news-intelligence-pipeline/
├── airflow/
│   └── dags/
│       ├── gdelt_ingestion.py      # Data ingestion DAG
│       └── gemini_processing.py    # AI processing DAG
├── spark/
│   └── jobs/
│       └── gdelt_processor.py      # Spark transformation job
├── gemini/
│   └── summarizer.py               # Gemini AI integration
├── streamlit/
│   ├── app.py                      # Main dashboard
│   └── pages/
│       ├── 1_map_view.py           # Geographic map
│       ├── 2_analytics.py          # Deep analytics
│       ├── 3_search_export.py      # Search & export
│       └── 4_system_stats.py       # System statistics
├── analytics/
│   ├── queries.sql                 # DuckDB queries
│   └── test_queries.py             # Analytics tests
├── data/
│   ├── bronze/                     # Raw ingested data
│   ├── silver/                     # Cleaned data
│   └── gold/                       # AI-enriched data
├── docker-compose.yml              # Infrastructure setup
├── requirements.txt                # Python dependencies
└── README.md
```

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 2.7.1 | Workflow management |
| **Processing** | Apache Spark 3.4.1 | Distributed data processing |
| **AI/ML** | Google Gemini 2.0 Flash | Text summarization |
| **Database** | PostgreSQL 13 | Airflow metadata |
| **Analytics** | DuckDB | Fast analytical queries |
| **Visualization** | Streamlit + Plotly | Interactive dashboard |
| **Containerization** | Docker & Docker Compose | Service orchestration |
| **Data Format** | Apache Parquet | Columnar storage |

## 📊 Performance Metrics

- **Data Ingestion:** 1,904 events in ~2 minutes
- **Spark Processing:** 1,835 events in ~30 seconds (96.4% quality retention)
- **AI Processing:** 1,835 summaries in ~2.5 hours
- **Article Fetch Rate:** 64% success rate
- **Total Cost:** ~$0.08 for complete pipeline run
- **Dashboard Load Time:** <2 seconds for 1,835 events

## 🎨 Dashboard Features

### Main Dashboard
- Real-time KPI metrics
- Event distribution by country
- Sentiment analysis charts
- Top actors visualization
- AI-generated summaries

### Map View
- Interactive global event map
- Color-coded by sentiment
- Size-scaled by article count
- Click-through to event details

### Analytics
- Sentiment by country scatter plots
- Event intensity distribution
- Top actors comparison
- Media coverage analysis
- Correlation visualizations

### Search & Export
- Advanced search across all fields
- Multi-filter support
- Export to CSV/JSON/TXT
- Bulk download summaries

### System Stats
- Pipeline performance metrics
- Data quality indicators
- AI processing statistics
- Cost tracking
- Layer-by-layer data sizes

## 🔐 Security & Best Practices

- ✅ API keys stored in `.env` (not in Git)
- ✅ Environment variables properly managed
- ✅ Read-only file system mounts where applicable
- ✅ Minimal container privileges
- ✅ Rate limiting for API calls
- ✅ Error handling and retry logic

## 📈 Future Enhancements

- [ ] Add more data sources (Twitter, Reddit, RSS feeds)
- [ ] Implement real-time streaming with Kafka
- [ ] Add sentiment trend analysis over time
- [ ] Build entity relationship graphs with NetworkX
- [ ] Implement RAG (Retrieval Augmented Generation) for Q&A
- [ ] Add email alerts for significant events
- [ ] Create automated reporting
- [ ] Deploy on AWS/GCP with Terraform

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **GDELT Project** for providing open global news data
- **Google** for Gemini API access
- **Apache Foundation** for Spark and Airflow
- **Streamlit** for the amazing dashboard framework

## 📧 Contact

**Shubhan Kadam**
- Email: dev.shubhankadam@gmail.com

---

**⭐ If you found this project helpful, please give it a star!**

*Built with ❤️ using Python, Spark, and AI*