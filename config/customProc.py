from src.analysis.processor import PreselProcessor, SkimProcessor, Processor

def switch_processors(sel_name) -> Processor:
    processors = {
        'skim': SkimProcessor,
        'preselect': PreselProcessor
    }

    return processors.get(sel_name, Processor)