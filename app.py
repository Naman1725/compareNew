from flask import Flask, request, jsonify
from compare_service import run_comparison_pipeline

app = Flask(__name__)

@app.route("/compare", methods=["POST"])
def compare_kpis():
    if "file" not in request.files:
        return jsonify({"error": "No ZIP file uploaded"}), 400

    zip_file = request.files["file"]
    if zip_file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    year = request.form.get("year")
    country = request.form.get("country", "All Countries")

    if not year:
        return jsonify({"error": "Missing 'year' parameter"}), 400

    try:
        year = int(year)
    except ValueError:
        return jsonify({"error": "'year' must be an integer"}), 400

    plot_data, summary, error = run_comparison_pipeline(zip_file.read(), year, country)

    if error:
        return jsonify({"error": error}), 400

    return jsonify({
        "summary": summary,
        "plots": plot_data
    })

@app.route("/test", methods=["GET"])
def test():
    return jsonify({"status": "Compare API working"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
