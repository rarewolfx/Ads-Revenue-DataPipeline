<div align="center">
<h1>Google Apps Revenue Data Pipeline</h1>
</div>

## Installation steps

1. Clone the Repo and install the requirements

```
git clone https://github.com/rarewolfx/Ads-Revenue-DataPipeline.git
cd Apps-Revenue-DataPipeline
pip install -r requirements.txt
```

2. Fill in the necessary credentials from Facebook and Google in the respective files and run
```
python cronjob.py
```
3. To run the Flask App do
```
python wsgi.py
```
