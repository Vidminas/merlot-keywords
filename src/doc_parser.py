import re
import struct
from olefile import OleFileIO

DOCNONASCIIPATTERN8 = re.compile(r"[\x7F-\xFF]")
DOCNONASCIIPATTERN16 = re.compile(r"[\u007F-\uFFFF]")
DOCTABLECLEAN = re.compile(r"[\x01-\x08]")
DOCSTRIPPATTERN = re.compile(r"\r")
DOCHYPERLINKPATTERN = re.compile(
    r"\x13.*HYPERLINK.*\"(?P<uri>.*)\".*\x14(?P<display>.*)\x15"
)


class ReaderError(Exception):
    pass


class DOCParser:
    """
    Class adapted from
    http://blog.digitally-disturbed.co.uk/2012/04/reading-microsoft-word-doc-files-in.html
    But my version uses the olefile library and is adapted for Python3
    """

    def __init__(self, document: OleFileIO):
        self.non_ascii_pattern8 = DOCNONASCIIPATTERN8
        self.non_ascii_pattern16 = DOCNONASCIIPATTERN16
        self.table_cleanup = DOCTABLECLEAN
        self.strip_pattern = DOCSTRIPPATTERN
        self.hyperlink_pattern = DOCHYPERLINKPATTERN
        self.file_version = "Unknown Version"
        self.document = document

    def extract_text(self) -> str:
        with self.document.openstream("WordDocument") as stream:
            doc_stream = stream.read()

        # The magic and version words define what version document this is
        # We dont handle pre-word 6 documents
        magic, version, flags = struct.unpack("<HH6xH", doc_stream[:12])
        if magic != 0xA5EC and magic != 0xA5DC:
            raise ReaderError("Invalid format - not a Word doc file")
        if version < 101:
            raise ReaderError("Very old doc file - cant handle before Word 95")
        elif version == 101 or version in range(103, 105):
            self.file_version = "Word 95"
            buff = self._process_word95(doc_stream)
        elif version >= 193:
            self.file_version = "Word 97 - 2003"
            buff = self._process_word97(doc_stream)
        else:
            raise ReaderError("Unknown version of Word")
        return buff

    def _clean_hyperlinks(self, buff: str):
        # Word marks up hyperlinks with a certain markup.
        # We want to strip this out, pull out the hyperlink text and uri,
        #  then add this to the text
        for match in self.hyperlink_pattern.finditer(buff):
            uri, display = match.groups()
            buff = self.hyperlink_pattern.sub(f"{display} (link: {uri})", buff, 1)
        return buff

    def _process_word95(self, doc_stream: bytes):
        # This version is so much easier to handle!
        # The text start offset and end offset are early on in the stream.
        # Pull them out, try clean up the text (seems to be ascii) and thats it
        text_start, text_end = struct.unpack_from("<II", doc_stream, 0x18)
        buff = doc_stream[text_start:text_end].decode("utf-8", errors="replace")
        buff = self.non_ascii_pattern8.sub("", buff)
        buff = self.table_cleanup.sub(" ", buff)
        buff = self._clean_hyperlinks(buff)
        return self.strip_pattern.sub("\r\n", buff)

    def _process_word97(self, doc_stream: bytes):
        if self.document.exists("1Table"):
            table_stream_name = "1Table"
        elif self.document.exists("0Table"):
            table_stream_name = "0Table"
        else:
            raise ReaderError("No Table stream found!")

        # Now, from the WordDocument stream pull out the size of the text
        # If there's any text in headers etc... then we need to add the extra
        #  amount of text along with 1 extra char (Dont know why the extra 1!!!)
        offset = 62
        count = struct.unpack_from("<H", doc_stream, offset)[0]
        offset += 2
        (
            text_size,
            foot_size,
            header_size,
            macro_size,
            annotation_size,
            endnote_size,
            textbox_size,
            headertextbox_size,
        ) = struct.unpack_from("12x8I", doc_stream, offset)

        # If any sizes other than text size are non zero, add them up and add 1
        if (
            foot_size
            or header_size
            or macro_size
            or annotation_size
            or endnote_size
            or textbox_size
            or headertextbox_size
        ):
            final_cp = (
                text_size
                + foot_size
                + header_size
                + macro_size
                + annotation_size
                + endnote_size
                + textbox_size
                + headertextbox_size
                + 1
            )
        else:
            final_cp = text_size

        # Skip across some unused structures to get an offset to the table stream
        offset += count * 4
        offset += (66 * 4) + 2  # Add offset from main block + count variable
        clx_offset, clx_size = struct.unpack_from("<II", doc_stream, offset)

        with self.document.openstream(table_stream_name) as stream:
            table_stream = stream.read()

        magic, size = struct.unpack_from("<BH", table_stream, clx_offset)
        if magic != 0x02:
            raise ReaderError("Not a valid clxt in the table stream")

        # Now read a list of cp offsets showing how the text is broken up
        cp_list = []
        offset = clx_offset + 5
        for i in range(size // 4):
            cp = struct.unpack_from("<I", table_stream, offset)[0]
            cp_list.append(cp)
            offset += 4
            if cp == final_cp:
                break
        if i == (size // 4) - 1:
            raise ReaderError("Parse error - doc file has no final cp")

        # For each cp offset we need to see if the text is 8 or 16 bit, get a
        # stream offset and process the text chunk
        buff = ""
        for i in range(len(cp_list[:-1])):
            fc = struct.unpack_from("<2xI", table_stream, offset)[0]
            stream_offset = int(fc & (0xFFFFFFFF >> 2))
            compressed = bool(fc & (0x01 << 30))
            next_cp = cp_list[i + 1]
            cp = cp_list[i]
            buff += self._process_block97(
                stream_offset, cp, next_cp, compressed, doc_stream
            )
            offset += 8

        return self.strip_pattern.sub("\r\n", buff)

    def _process_block97(
        self,
        text_offset: int,
        cp: int,
        next_cp: int,
        compressed: bool,
        doc_stream: bytes,
    ):
        # For each text block we need to read the data and try clean it up.
        # The data has special markup for tables and hyperlinks as well as other
        # stuff that can be quite nasty of you dont clean it up
        if compressed:
            text_offset //= 2
            last = (text_offset) + next_cp - cp - 1
            buff = doc_stream[text_offset:last].decode("utf-8", errors="replace")
            buff = self.non_ascii_pattern8.sub("", buff)
            buff = self.table_cleanup.sub(" ", buff)
            return self._clean_hyperlinks(buff)
        else:
            last = text_offset + 2 * (next_cp - cp)
            buff = doc_stream[text_offset:last].decode("utf-16", errors="replace")
            buff = self._clean_hyperlinks(buff)
            buff = self.non_ascii_pattern16.sub("", buff)
            return self.table_cleanup.sub(" ", buff)