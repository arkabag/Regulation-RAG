import re
import textract

class PdfParser(object):
    def __init__(self, filepath):
        self.rawtext = textract.process(filepath)  # read the content of pdf as text
        self.paragraphs = re.split('\s{3,}', self.rawtext.decode("utf-8"))  # use four space as paragraph delimiter to convert the text into list of paragraphs.

        self.dedupe()
        self.join_paragraphs()
        self.remove_space()

    def dedupe(self):
        para_deduped = []
        [para_deduped.append(x) for x in self.paragraphs if x not in para_deduped]
        self.paragraphs = para_deduped

    def join_paragraphs(self):
        para_joined = []
        last_para = ''
        for para in self.paragraphs:
            if para.endswith('.'):
                para_joined.append(last_para + ' ' + para)
                last_para = ''
            else:
                last_para += ' ' + para

        self.paragraphs = para_joined

    def remove_space(self):
        para_cleaned = []
        for para in self.paragraphs:
            para_cleaned.append(re.sub(r"\s+", ' ', para))

        self.paragraphs = para_cleaned

    def as_text(self):
        return '\n\n'.join(self.paragraphs)

    def get_paragraphs(self):
        return self.paragraphs

if __name__ == '__main__':
    parser = PdfParser('./documents/MultipleDwellingLaw.pdf')

    document_as_text = parser.as_text()
    paragraphs = parser.get_paragraphs()

    print(document_as_text)