from flask import Flask, request, jsonify
import os
import json
from preprocess import sets_create
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

save_dir = 'C:\\Users\\Umaro\\OneDrive\\Рабочий стол\\datasets'
# Создаём папку datasets, если она не существует
os.makedirs(save_dir, exist_ok=True)

@app.route('/process', methods=['POST'])
def process_files():
    if not request.json or 'directory' not in request.json:
        return jsonify({'error': 'Directory not provided'}), 400

    directory = request.json['directory']

    # Проверяем, что директория существует
    if not os.path.isdir(directory):
        return jsonify({'error': 'Directory does not exist'}), 400

    # Получаем список файлов CSV в директории
    csv_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]

    if not csv_files:
        return jsonify({'error': 'No CSV files found in the directory'}), 400

    try:
        # Обрабатываем CSV файлы
        train_df, val_df, test_df = sets_create(csv_files)

        # Преобразуем датафреймы в формат, подходящий для передачи
        data_to_save = {
            'train_data': train_df.to_dict(orient='records'),
            'valid_data': val_df.to_dict(orient='records'),
            'test_data': test_df.to_dict(orient='records')
        }

        # Путь к файлу для сохранения
        output_path = os.path.join(save_dir, 'processed_data.json')

        # Сохраняем результат в JSON файл
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, ensure_ascii=False, indent=4)

        # Возвращаем успешный ответ
        return jsonify({
            'message': 'Processing successful. Results saved to JSON file.',
            'output_file': output_path
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5003)
