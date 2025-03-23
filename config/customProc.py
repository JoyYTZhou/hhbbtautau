from src.analysis.processor import Processor, SkimProcessor, PreselProcessor

def switch_processors(sel_name) -> Processor:
    processors = {
        'skim': SkimProcessor,
        'preselect': PreselProcessor
    }

    return processors.get(sel_name, Processor)