import os
import csv
import datetime
from typing import Optional, List

from cloud_cover import load_and_trim, process_image_rb_ratio, calculate_cloud_cover
from download_sky_images import download_sky_camera_images


SKY_IMAGE_DIR = "sky_camera_images"
METEOBLUE_CSV = os.path.join("Data", "meteoblue_data.csv")
OUTPUT_CSV = "training_data_with_cloud_cover.csv"


def _parse_meteoblue_datetime(cell: str) -> Optional[datetime.datetime]:
    """Parse meteoblue date_time field.

    Expected format: YYYYMMDDTHHMM (e.g. 20190523T0900).
    Returns naive datetime in local camera time (same as filenames).
    """
    if not cell or len(cell) < 13:
        return None
    date_part = cell[:8]
    if not represents_int(date_part):
        return None
    # Some files use 'T' separator.
    try:
        return datetime.datetime.strptime(cell[:13], "%Y%m%dT%H%M")
    except ValueError:
        return None


def _sky_image_filename(dt: datetime.datetime) -> str:
    """Map datetime to local filename used by download_sky_images.py.

    download_sky_images.py uses str(datetime_obj) then replaces whitespace/':' with '-'
    and drops the timezone suffix, ending up like: 2019-05-23-09-00-0.jpg
    """
    return f"{dt:%Y-%m-%d-%H-%M}-0.jpg"


def _cloud_cover_from_file(path: str) -> Optional[float]:
    if not os.path.exists(path):
        return None
    try:
        trimmed = load_and_trim(path)
        rb = process_image_rb_ratio(trimmed)
        return float(calculate_cloud_cover(rb))
    except Exception:
        # Keep dataset generation resilient; missing/bad images become blank cells.
        return None


def _ensure_images_downloaded(start_date: datetime.date, end_date: datetime.date) -> None:
    """Download images for a date range (inclusive) if the folder is empty/missing."""
    os.makedirs(SKY_IMAGE_DIR, exist_ok=True)

    # If folder already has some images, don't force a full re-download.
    try:
        has_any = any(name.lower().endswith(".jpg") for name in os.listdir(SKY_IMAGE_DIR))
    except FileNotFoundError:
        has_any = False

    if has_any:
        return

    # download_sky_camera_images expects YYYY-MM-DD strings.
    download_sky_camera_images(start_date.isoformat(), end_date.isoformat())


def main():
    start_date = "2019-05-23"
    end_date = "2019-05-23"

    start_d = datetime.date(int(start_date[:4]), int(start_date[5:7]), int(start_date[8:]))
    end_d = datetime.date(int(end_date[:4]), int(end_date[5:7]), int(end_date[8:]))

    # Date range inclusive
    dates: List[datetime.date] = []
    cur = start_d
    delta = datetime.timedelta(days=1)
    while cur <= end_d:
        dates.append(cur)
        cur += delta

    # Meteoblue rows we care about
    times = {"0900", "1000", "1100", "1200", "1300", "1400", "1500"}

    # Download images if needed.
    _ensure_images_downloaded(start_d, end_d)

    # Ensure output folder exists (current dir is fine, but keep consistent behavior)
    out_dir = os.path.dirname(OUTPUT_CSV)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as wfile:
        writer = csv.writer(wfile)

        with open(METEOBLUE_CSV, newline="", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                if not row:
                    continue

                # Preserve the first 3 metadata rows, but add our extra columns.
                if row[0] in {"variable", "unit", "level"}:
                    writer.writerow(row + ["cloud_cover_t", "cloud_cover_t+5m", "cloud_cover_t+10m"])
                    continue

                dt = _parse_meteoblue_datetime(row[0])
                if dt is None:
                    continue

                if dt.date() not in dates:
                    continue

                hhmm = dt.strftime("%H%M")
                if hhmm not in times:
                    continue

                # Compute cloud cover for t, t+5, t+10
                covers: List[Optional[float]] = []
                for minutes in (0, 5, 10):
                    dt_i = dt + datetime.timedelta(minutes=minutes)
                    img_path = os.path.join(SKY_IMAGE_DIR, _sky_image_filename(dt_i))
                    covers.append(_cloud_cover_from_file(img_path))

                def fmt(x: Optional[float]) -> str:
                    return "" if x is None else f"{x:.6f}"

                writer.writerow(row + [fmt(c) for c in covers])


def represents_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    main()