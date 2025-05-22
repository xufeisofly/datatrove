import re

from datatrove.data import Document
from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.text import split_into_sentences
from datatrove.utils.typeshelper import Languages


# def modify_doc_by_paragraph(
#         doc: Document,
#         valid_line_in_paragraph_ratio,
#         max_non_alpha_words_ratio,
#         whitelist_chars, 
#         use_whitelist,
#         min_word_num,        
# ):
#     text = doc.text
#     paras = text.split('\n\n')
#     new_paras = []
#     for para in paras:
#         lines = para.split('\n')
#         total_word_num = word_num_of_lines(lines)
#         invalid_line_word_num = 0
#         for line in lines:
#             if not is_line_valid(line,
#                                  max_non_alpha_words_ratio=max_non_alpha_words_ratio,
#                                  whitelist_chars=whitelist_chars,
#                                  use_whitelist=use_whitelist,
#                                  min_word_num=min_word_num):
#                 invalid_line_word_num += word_num_of_line(line)

#         if (word_num_of_lines(lines)-invalid_line_word_num) / total_word_num >= valid_line_in_paragraph_ratio:
#             new_paras.append(para)
        

#     new_text = '\n\n'.join(new_paras)
#     doc.text = new_text
#     if text != new_text:
#         add_modifiers_to_meta(doc, 'preprocess_beta1')


def check_javascript(text):
    if "javascript" not in text:
        return False
    if "enable" in text:
        return True
    if "disable" in text:
        return True
    if "require" in text:
        return True
    if "activate" in text:
        return True
    if "browser" in text:
        return True
    return False


def is_counter(input_text):
    pattern = r'^\d+\s+likes$'
    return bool(re.match(pattern, input_text))        


def line_filtering(line, max_uppercase_ratio, min_word_cnt_per_line) -> tuple[bool, int]:
    """
    return:
    1. bool: whether the line should be filtered or not
    2. int: words count in the removed line 
    """
    # Normalize the line text
    line_norm = line.strip().lower()
    if not line_norm:
        return True, 0
    word_cnt_line = len(line_norm.split())

    # 1.1 Remove lines not ending up in a terminal punctuation mark
    # if not line_norm.endswith((".", "?", "!", '"')):  # Done: Check if we need to use this
    #     return False, "C4_not_ending_with_terminal_punctuation"

    # 1.2 Remove lines containing word "javascript"
    # if "javascript" in line_norm:  # Done: refine the strategy
    if check_javascript(line_norm):
        return True, 0  #, "1.2_C4_javascript"
    
    # Set the default for fraction_of_words_corrected_in_lines
    # Do not include the characters corrected by javascript into the counting

    # 1.3.1 Remove lines of uppercase characters only
    num_uppercase = sum(char.isupper() for char in line)
    if num_uppercase / len(line) > max_uppercase_ratio:
        return True, word_cnt_line
        
    # if line.isupper():
    #     return True, word_cnt_line  #, "1.3.1_RefinedWeb_uppercase_only"
    # 1.3.2 Remove lines of numerical characters
    if line_norm.isdigit():
        return True, word_cnt_line  #, "1.3.2_RefinedWeb_digits_only"
    # 1.3.3 Remove lines of counter
    if is_counter(line_norm):
        return True, word_cnt_line  #, "1.3.3_RefinedWeb_is_counter"
    # 1.3.4 Remove lines with only a few word
    if word_cnt_line < min_word_cnt_per_line:  # TODO: decide the threshold
        return True, word_cnt_line  #, "1.3.4_RefinedWeb_line_too_short"


    return False, 0  #, None


class LineRemovalFilter(BaseFilter):
    name = "🌍 Line Removal Filter"
    _requires_dependencies = []
    
    def __init__(
            self,
            max_removed_ratio: float = 0.05,
            max_uppercase_ratio: float = 0.99,
            min_word_cnt_per_line: int = 2,
            num_of_sentences: int = 3,
            exclusion_writer: DiskWriter = None,
            store_new_text = False,
            language: str = Languages.english,
    ):
        super().__init__(exclusion_writer)
        self.max_removed_ratio = max_removed_ratio
        self.max_uppercase_ratio = max_uppercase_ratio
        self.min_word_cnt_per_line = min_word_cnt_per_line
        self.num_of_sentences = num_of_sentences
        self.store_new_text = store_new_text
        self.language = language


    def filter(self, doc: Document) -> bool | tuple[bool, str]:
        text = doc.text
        lines = text.split("\n")
        
        new_lines = []
        fraction_of_words_corrected_in_lines = 0
        num_sentences = 0
        
        for line in lines:
            # line removal
            is_filtered, removed_words_cnt = line_filtering(
                line,
                max_uppercase_ratio=self.max_uppercase_ratio,
                min_word_cnt_per_line=self.min_word_cnt_per_line)
            
            if not is_filtered:
                new_lines.append(line)
                sentences = split_into_sentences(line, self.language)
                num_sentences += len(sentences)
            # 统计被删除的单词数
            fraction_of_words_corrected_in_lines += removed_words_cnt

        total_words_cnt = len(text.split())
        if total_words_cnt and fraction_of_words_corrected_in_lines / total_words_cnt  > self.max_removed_ratio:
            return False, 'too_many_removed_lines'
        if num_sentences < self.num_of_sentences:
            return False, 'too_few_sentences'

        # line-wise doc filtering
        for line in new_lines:
            # line-wise doc filtering
            if "lorem ipsum" in line.lower():
                return False, "lorem_ipsum"        

        doc.text = "\n".join(new_lines)
        if self.store_new_text:
            doc.metadata['new_text'] = doc.text

        return True

        
