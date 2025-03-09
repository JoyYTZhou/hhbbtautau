from src.analysis.processor import Processor, SkimProcessor

def switch_processors(sel_name):
    processors = {
        'skim': SkimProcessor,
        'selection': Processor
    }

    return processors.get(sel_name, Processor)