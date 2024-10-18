import re

import scrapy
from scrapy.exceptions import CloseSpider

from ..items import DocumentItem


class PACASpider(scrapy.Spider):

    name = "DREAL PACA Scraper"

    # allowed_domains = ["paca.developpement-durable.gouv.fr"]

    start_urls = [
        "https://www.paca.developpement-durable.gouv.fr/acces-direct-aux-avis-et-aux-decisions-suite-a-r2853.html"
    ]

    upload_limit_attained = False

    def check_upload_limit(self):
        """Closes the spider if the upload limit is attained."""
        if self.upload_limit_attained:
            raise CloseSpider("Closed due to max documents limit.")

    def parse(self, response):
        """Parse the starting page"""

        years_links = response.css("#contenu div.fr-collapse div>a")

        for link in years_links:

            # Get link text and url
            link_text = link.css("::text").get()
            link_url = link.attrib["href"]

            # Extract year
            year_match = re.search("Dossiers (20\d\d)", link_text)
            year = int(year_match.group(1))

            if year == self.target_year:

                self.logger.info(f"Parsing year {year} ({link_url})")

                yield response.follow(link_url, callback=self.parse_departments_list)

    def parse_departments_list(self, response):
        """Parse the departments selection page of a year."""

        dept_links = response.css("#contenu a.fr-tile__link")

        for link in dept_links:
            link_text = link.css("::text").get()
            link_url = link.attrib["href"]

            # self.logger.info(f"Seen: {link_text} ({link_url})")

            yield response.follow(
                link_url,
                callback=self.parse_projects_list,
                cb_kwargs=dict(dept=link_text, page=1),
            )

    def parse_projects_list(self, response, dept, page):
        """Parse projects list for a year & department."""

        self.logger.info(f"Scraping {dept.split(' - ')[1]}, page {page}")

        # yield project pages

        projects_links = response.css("#contenu .fr-card__link")

        for link in projects_links:
            link_text = link.css("::text").get()
            link_url = link.attrib["href"]

            # print(f"Seen: {link_text} at {link_url}")
            yield response.follow(
                link_url,
                callback=self.parse_project_page,
                cb_kwargs=dict(dept=dept),
            )

        # next page

        next_page_link = response.css(
            "#contenu .fr-pagination__list .fr-pagination__link--next[href]"
        )

        if next_page_link:

            next_page_url = next_page_link.attrib["href"]

            yield response.follow(
                next_page_url,
                callback=self.parse_projects_list,
                cb_kwargs=dict(dept=dept, page=page + 1),
            )

    def parse_project_page(self, response, dept):
        """Parse the page of a project."""

        file_links = response.css("#contenu div.fr-downloads-group a.fr-download__link")

        if file_links:

            project = response.css("h1.titre-article::text").get()

            raw_info = response.css(".texte-article").css("*::text").extract()

            info = "".join([x.lstrip() for x in raw_info if x.strip()]).strip()

            if not info:
                info = response.css(".fr-text--lead").css("*::text").extract()
                if info:
                    info = info[0].strip()

            if not info:
                info = ""

            info_linestart = [
                "Rubrique(s) concernée(s) :",
                "Pétitionnaire :",
                "Date de réception :",
                "Dossier complet le :",
                "Décision :",
                "Dossier reçu le :",
                "Recours gracieux du :",
            ]

            for e in info_linestart:
                info = info.replace(e, "\n" + e)

            # Process files

            for link in file_links:
                link_text = link.css("::text").get().strip()
                link_url = link.attrib["href"]

                full_link_url = response.urljoin(link_url)

                if full_link_url not in self.event_data:

                    doc_item = DocumentItem(
                        title=link_text,
                        source_page_url=response.request.url,
                        project=project,
                        year=self.target_year,
                        authority="Préfecture de région Provence-Alpes-Côte d'Azur",
                        category_local="Décisions suite à examen au cas par cas des projets",
                        source_scraper="DREAL PACA Scraper",
                        full_info=info,
                        department=dept.split(" - ")[0],
                        source="www.paca.developpement-durable.gouv.fr",
                    )

                    yield response.follow(
                        link_url,
                        method="HEAD",
                        callback=self.parse_document_headers,
                        cb_kwargs=dict(doc_item=doc_item),
                    )
                else:
                    self.logger.debug(f"File already scraped: {full_link_url}")

    def parse_document_headers(self, response, doc_item):

        self.check_upload_limit()

        doc_item["source_file_url"] = response.request.url

        doc_item["publication_lastmodified"] = response.headers.get(
            "Last-Modified"
        ).decode("utf-8")

        yield doc_item
