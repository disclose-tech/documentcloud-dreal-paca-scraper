# Item Pipelines

import datetime
import re
import os
from urllib.parse import urlparse
import logging
import json

from scrapy.exceptions import DropItem

from documentcloud.constants import SUPPORTED_EXTENSIONS

from .log import SilentDropItem


class ParseDatePipeline:
    """Parse dates from scraped data."""

    def process_item(self, item, spider):
        """Parses date from the extracted string"""

        # Publication date

        publication_dt = datetime.datetime.strptime(
            item["publication_lastmodified"], "%a, %d %b %Y %H:%M:%S %Z"
        )

        # item["publication_timestamp"] = publication_dt.isoformat() + "Z"

        item["publication_date"] = publication_dt.strftime("%Y-%m-%d")
        item["publication_time"] = publication_dt.strftime("%H:%M:%S UTC")
        item["publication_datetime"] = (
            item["publication_date"] + " " + item["publication_time"]
        )

        return item


class CategoryPipeline:
    """Attributes the final category of the document."""

    def process_item(self, item, spider):

        if "cas par cas" in item["category_local"].lower():
            item["category"] = "Cas par cas"

        return item


class SourceFilenamePipeline:
    """Adds the source_filename field based on source_file_url."""

    def process_item(self, item, spider):

        path = urlparse(item["source_file_url"]).path

        item["source_filename"] = os.path.basename(path)

        return item


class BeautifyPipeline:
    def process_item(self, item, spider):
        """Beautify & harmonize project names & document titles."""

        # Full info
        # Beautified to simplify regex to extract municipalities below

        item["full_info"] = (
            item["full_info"].replace(" ", " ").replace("’", "'").replace("  ", " ")
        )

        # Project

        item["project"] = item["project"].strip()
        item["project"] = item["project"].replace(" ", " ").replace("’", "'")
        item["project"] = item["project"].rstrip(".,")

        # Reformating
        # Name of the project (ID)
        project_match = re.match(r"([A-Za-z0-9]+)_? *(?::|-) *(.*)", item["project"])
        project_id, project_name = project_match.groups()

        # Remove quotation marks
        if project_name.startswith('"') and project_name.endswith('"'):
            project_name = project_name.strip('"')

        item["project"] = f"{project_name.strip()} ({project_id.strip().upper()})"
        item["project"] = item["project"][0].upper() + item["project"][1:]

        municipalities = re.search(
            r"Commune\(s\) du projet : ?(.*)\n", item["full_info"]
        )

        if municipalities:
            municipalities = municipalities.group(1).replace(" ; ", ", ").strip()

            # Missing space before opening parenthesis
            # Gap(05) -> Gap (05)
            municipalities = re.sub(r"(\S)\(", r"\1 (", municipalities)
            # Missing space before closing parenthesis
            # Gap(05) -> Gap (05)
            municipalities = re.sub(r"(\s)\)", r")", municipalities)

            # Different template
            # 05 GAP -> Gap (05)
            municipalities = re.sub(
                r"^(\d{2})\s*[-]?\s*(.+?)(?:\s*\(\d{2}\))?$",
                r"\2 (\1)",
                municipalities,
            )

            # Add department number if missing
            if not re.search(r"\d\d\)$", municipalities):
                municipalities += f" ({item['department']})"

            item["project"] = item["project"] + " - " + municipalities

        item["project"] = item["project"].strip()

        # Title

        # item["title"] = item["title"].strip()
        # item["title"] = item["title"].replace("  ", " ").replace("’", "'")
        # item["title"] = item["title"].rstrip(".,")

        item["title"] = item["title"].replace("  ", " ")

        # Format title
        # F093XXXXX Doc name
        split_title = item["title"].split(" ")

        if len(split_title) > 1:

            if split_title[0].lower().startswith("f09"):

                # Project id in uppercase
                split_title[0] = split_title[0].upper()

                # Capitalize next word of title
                split_title[1] = split_title[1][0].upper() + split_title[1][1:]

            else:
                split_title[0] = split_title[0][0].upper() + split_title[0][1:]

            item["title"] = " ".join(split_title)

        else:
            if item["title"].strip().lower().startswith("f09"):
                item["title"] = item["title"].upper().strip()
            else:
                item["title"] = item["title"][0].upper() + item["title"][1:]

        # Replace "Ap" by "Arrêté préfectoral"
        item["title"] = re.sub(
            r"(F0\w{8,10}(?:(?:-\d| \d))?) Ap\b",
            r"\1 Arrêté préfectoral",
            item["title"],
        )

        return item


class UnsupportedFiletypePipeline:

    def process_item(self, item, spider):

        filename, file_extension = os.path.splitext(item["source_filename"])
        file_extension = file_extension.lower()

        if file_extension not in SUPPORTED_EXTENSIONS:
            # Drop the item
            raise DropItem("Unsupported filetype")
        else:
            return item


class UploadLimitPipeline:
    """Sends the signal to close the spider once the upload limit is attained."""

    def open_spider(self, spider):
        self.number_of_docs = 0

    def process_item(self, item, spider):
        self.number_of_docs += 1

        if spider.upload_limit == 0 or self.number_of_docs < spider.upload_limit + 1:
            return item
        else:
            spider.upload_limit_attained = True
            print("Upload limit attained. Closing spider...")
            raise SilentDropItem("Upload limit exceeded.")


class UploadPipeline:
    """Upload document to DocumentCloud & store event data."""

    def open_spider(self, spider):
        documentcloud_logger = logging.getLogger("documentcloud")
        documentcloud_logger.setLevel(logging.WARNING)

        if not spider.dry_run:
            try:
                spider.logger.info("Loading event data from DocumentCloud...")
                spider.event_data = spider.load_event_data()
            except Exception as e:
                raise Exception("Error loading event data").with_traceback(
                    e.__traceback__
                )
                sys.exit(1)
        else:
            # Load from json if present
            try:
                spider.logger.info("Loading event data from local JSON file...")
                with open("event_data.json", "r") as file:
                    data = json.load(file)

                    spider.event_data = data
            except:
                spider.event_data = None

        if spider.event_data:
            spider.logger.info(
                f"Loaded event data ({len(spider.event_data)} documents)"
            )
        else:
            spider.logger.info("No event data was loaded.")
            spider.event_data = {}

    def process_item(self, item, spider):

        try:
            if not spider.dry_run:
                spider.client.documents.upload(
                    item["source_file_url"],
                    project=spider.target_project,
                    title=item["title"],
                    description=item["project"],
                    source=item["source"],
                    language="fra",
                    access=item["access"],
                    data={
                        "authority": item["authority"],
                        # "region": item["region"],
                        "category": item["category"],
                        "category_local": item["category_local"],
                        "source_scraper": item["source_scraper"],
                        "source_file_url": item["source_file_url"],
                        "event_data_key": item["source_file_url"],
                        "source_page_url": item["source_page_url"],
                        "source_filename": item["source_filename"],
                        "publication_date": item["publication_date"],
                        "publication_time": item["publication_time"],
                        "publication_datetime": item["publication_datetime"],
                        "year": str(item["year"]),
                    },
                )
                spider.logger.info(
                    f"Uploaded {item['source_file_url']} to DocumentCloud"
                )
        except Exception as e:
            raise Exception("Upload error").with_traceback(e.__traceback__)
        else:  # No upload error, add to event_data
            now = datetime.datetime.now().isoformat()
            spider.event_data[item["source_file_url"]] = {
                "last_modified": item["publication_datetime"],
                "last_seen": now,
            }
            # # Save event data after each upload
            if spider.run_id and not spider.dry_run:  # only from the web interface
                spider.store_event_data(spider.event_data)

        return item

    def close_spider(self, spider):
        """Store event data when the spider closes."""

        if not spider.dry_run and spider.run_id:
            spider.store_event_data(spider.event_data)
            spider.logger.info(
                f"Uploaded event data ({len(spider.event_data)} documents)"
            )

            if spider.upload_event_data:
                with open(filename, "w+") as event_data_file:
                    json.dump(spider.event_data, event_data_file)
                    spider.upload_file(event_data_file)
                spider.logger.info(
                    f"Uploaded event data to the Documentcloud interface."
                )

        if not spider.run_id:
            with open("event_data.json", "w") as file:
                json.dump(spider.event_data, file)
                spider.logger.info(
                    f"Saved file event_data.json ({len(spider.event_data)} documents)"
                )


class MailPipeline:
    """Send scraping run report."""

    def open_spider(self, spider):
        self.scraped_items = []

    def process_item(self, item, spider):

        self.scraped_items.append(item)

        return item

    def close_spider(self, spider):

        def print_item(item, error=False):
            item_string = f"""
            title: {item["title"]}
            project: {item["project"]}
            authority: {item["authority"]}
            category: {item["category"]}
            category_local: {item["category_local"]}
            publication_date: {item["publication_date"]}
            source_file_url: {item["source_file_url"]}
            source_page_url: {item["source_page_url"]}
            """

            return item_string

        subject = f"DREAL PACA Scraper {str(spider.target_year)} (New: {len(self.scraped_items)}) [{spider.run_name}]"

        start_content = f"DREAL PACA Scraper Addon Run {spider.run_id}"

        scraped_items_content = (
            f"SCRAPED ITEMS ({len(self.scraped_items)})\n\n"
            + "\n\n".join([print_item(item) for item in self.scraped_items])
        )

        content = "\n\n".join([start_content, scraped_items_content])

        if not spider.dry_run:
            spider.send_mail(subject, content)
