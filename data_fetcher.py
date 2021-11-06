import httpx
import pandas as pd
from bs4 import BeautifulSoup


class DataFetcher:
    def __init__(self, url: str):
        self.url: str = url
        self.df: pd.DataFrame = None

    async def parse(self, table_name):
        content = await self._fetch_content()
        values = self._parse_table(content, table_name)
        df = pd.DataFrame(values[1:], columns=values[0])  # works if there are headers
        self.df = df
        return self.df

    async def _fetch_content(self):
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(self.url)
                res.raise_for_status()
                content = res.content
            except httpx.HTTPStatusError as e:
                raise Exception(
                    f"Something went wrong with downloading {self.url}, status_code:{e.response.status_code}")
        return content

    # taken from stackoverflow: https://stackoverflow.com/questions/2935658/beautifulsoup-get-the-contents-of-a-specific-table
    def _parse_table(self, content: str, table_name: str):
        def __get_text(tr: str, coltag: str = "td"):
            return [td.get_text(strip=True) for td in tr.find_all(coltag)]

        bs = BeautifulSoup(content, "html.parser")
        table = bs.find(lambda tag: tag.name == "table" and tag.has_attr("id") and tag["id"] == table_name)
        if not table:
            raise Exception("Empty table")
        rows = []
        trs = table.find_all("tr")
        header_row = __get_text(trs[0], "th")
        if header_row:
            trs = trs[1:]
        else:
            header_row = [f"header_{x}" for x in range(len(trs[1]))]
            rows.append(header_row)

        rows.append(header_row)

        for tr in trs:
            rows.append(__get_text(tr, "td"))
        return rows

    # ugly, but ok
    def save_to_file(self, file_type, file_name):
        output_file = f"{file_name}.{file_type}"
        if file_type == "csv":
            self.df.to_csv(output_file, index=False, encoding="utf-8")
        elif file_type == "json":
            with open(output_file, "w", encoding="utf-8") as fout:
                self.df.to_json(fout, index=False, force_ascii=False, orient="split")
        elif file_type == "xlsx":
            self.df.to_excel(output_file, index=False, encoding="utf-8", )
        elif file_type == "html":
            with open(output_file, "w", encoding="utf-8") as fout:
                fout.writelines('<meta charset="UTF-8">\n')
                html = self.df.to_html(index=False)
                fout.write(html)
        return output_file

