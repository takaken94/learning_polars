import polars as pl
import logging
from pathlib import Path

# ロギング設定
def setup_logging() -> None:
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / "app.log"

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    # ルートロガー
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # コンソール出力
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # ファイル出力
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # 二重登録防止（重要）
    if not root_logger.handlers:
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

logger = logging.getLogger(__name__)

"""
メイン処理
"""
def main():
    # ロギング設定
    setup_logging()

    # --- 設定 ---
    # 入力ファイル
    in_file_path = "data/22_shizuoka_all_20251226.csv"
    in_file_encoding = "utf-8"

    # 列名定義ファイル
    col_name_file_path = "data/column_name.txt"
    col_name_file_encoding = "utf-8"
    target_col_name = "column_name_en"


    # 出力ファイル
    out_csv_file_path = "data/22_shizuoka_all.csv"
    out_parq_file_path = "data/22_shizuoka_all.parquet"

    # --- 処理 ---
    # 入力ファイルの読み込み
    try:
        df = pl.read_csv(
            source=in_file_path,
            encoding=in_file_encoding,
            has_header=False,
            separator=",",
            infer_schema_length=0, # 全ての列を str として読み込む pandas の dtype=str に相当
        )
    except FileNotFoundError:
        logging.error(f"ファイルが見つかりません: {in_file_path}")
        exit(1)
    except Exception as e:
        logging.error(f"ファイル読み込み中にエラーが発生しました: {e}")
        exit(1)
    logging.info(f"データの読み込み完了 {df.shape}")

    # 列名マスタの読み込み
    try:
        col_name_df = pl.read_csv(
            source=col_name_file_path,
            encoding=col_name_file_encoding,
            has_header=True,
            separator="\t",
        )
    except FileNotFoundError:
        logging.error(f"列名定義ファイルが見つかりません: {col_name_file_path}")
        exit(1)
    except Exception as e:
        logging.error(f"列名定義ファイルの読み込み中にエラーが発生しました: {e}")
        exit(1)
    logging.info(f"列名定義ファイルの読み込み完了 {col_name_df.shape}")
    new_cols = col_name_df[target_col_name].to_list()

    # 列名の設定
    if len(df.columns) == len(new_cols):
        old_cols = df.columns
        df = df.rename(dict(zip(old_cols, new_cols)))
    else:
        logging.error("データフレームの列数と新しい列名の数が一致しません。データフレームの列数: {len(df.columns)}, 新しい列名の数: {len(new_cols)}")
        exit(1)

    # --- 処理 ---
    # データの保存
    try:
        df.write_csv(out_csv_file_path)
    except Exception as e:
        logging.error(f"CSVファイルの保存中にエラーが発生しました: {e}")

    try:
        df.write_parquet(out_parq_file_path)
    except Exception as e:
        logging.error(f"Parquetファイルの保存中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()