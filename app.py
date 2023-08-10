from flask import Flask, render_template
from data_handler import get_paras, extract_info, convert_to_df, combine_df
from collections import defaultdict

app = Flask(__name__)

@app.route("/")
def index():
    return render_template('index.html')

@app.route("/elig")
def elig(search_all=False):
    elig_result = get_paras('elig', [])
    de, me= extract_info(elig_result, defaultdict(list), [])
    elig_df = convert_to_df(de, "elig")
    if search_all: return elig_df
    elig_json = elig_df.to_json(orient='records')
    return render_template('elig.html', elig_data=elig_json)

@app.route("/med")
def med(search_all=False):
    med_result = get_paras('med', [])
    dm, mm= extract_info(med_result, defaultdict(list), [])
    med_df = convert_to_df(dm, "med")
    if search_all: return med_df
    med_json = med_df.to_json(orient='records')
    return render_template('med.html', med_data=med_json)

@app.route("/rx")
def rx(search_all=False):
    rx_result = get_paras('rx', [])
    dr, mr= extract_info(rx_result, defaultdict(list), [])
    rx_df = convert_to_df(dr, "rx")
    if search_all: return rx_df
    rx_json = rx_df.to_json(orient='records')
    return render_template('rx.html', rx_data=rx_json)


@app.route("/search_all")
def search_all():
    rx_df = rx(True)
    elig_df = elig(True)
    med_df = med(True)
    all_data = combine_df(elig_df, rx_df, med_df)
    all_data_json = all_data.to_json(orient='records')
    return render_template('search_all.html', data=all_data_json)

if __name__ == "__main__":
    app.run()