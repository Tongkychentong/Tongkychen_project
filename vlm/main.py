import sys
from datetime import datetime
from pathlib import Path

from vlm import VLM_infer


def read_prompt(file_path: Path) -> str:
	if not file_path.exists():
		return ""
	return file_path.read_text(encoding="utf-8").strip()


def list_image_files(images_dir: Path):
	valid_exts = {".jpg", ".jpeg", ".png", ".bmp", ".gif"}
	if not images_dir.exists():
		return []
	return sorted(
		[p for p in images_dir.iterdir() if p.is_file() and p.suffix.lower() in valid_exts]
	)


def save_results_to_excel(rows, output_path: Path):
	try:
		from openpyxl import Workbook
	except ImportError:
		print("缺少 openpyxl，请先安装：pip install openpyxl")
		sys.exit(1)

	wb = Workbook()
	ws = wb.active
	ws.title = "results"
	ws.append(["图片名称", "结果"])
	for image_name, result in rows:
		ws.append([image_name, result])
	wb.save(output_path)


def main():
	root = Path(__file__).parent
	images_dir = root / "images"
	system_prompt_file = root / "system_prompt.txt"
	user_prompt_file = root / "user_prompt,txt"

	system_prompt = read_prompt(system_prompt_file)
	user_prompt = read_prompt(user_prompt_file)

	images = list_image_files(images_dir)
	if not images:
		print(f"未在 {images_dir} 找到图片文件，请先添加待推理的图片")
		return

	results = []
	for image_path in images:
		try:
			response = VLM_infer(str(image_path), system_prompt, user_prompt)
		except Exception as exc:
			response = f"ERROR: {exc}"
		results.append((image_path.name, response))
		print(f"已完成: {image_path.name}")

	timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
	output_excel = root / f"vlm_results_{timestamp}.xlsx"
	save_results_to_excel(results, output_excel)
	print(f"结果已保存到 {output_excel}")


if __name__ == "__main__":
	main()
