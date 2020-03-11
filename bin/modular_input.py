import sys
import logging
import xml.dom.minidom
import time
import os, hashlib, json, random

# print out the configuration scheme
def arg_scheme(): 
    print("""<scheme>
    <title>My Simple Modular Input</title>
    <description>A Simple Modular Input</description>
    <use_external_validation>true</use_external_validation>
    <streaming_mode>simple</streaming_mode>
    <use_single_instance>true</use_single_instance>
    <endpoint>
        <args>
            <arg name="num_events">
                <title>Number of events to generate</title>
                <description>The number of events to generate each time the modular input runs.</description>
                <validation>is_nonneg_int('num_events')</validation>
                <data_type>number</data_type>
            </arg>
        </args>
    </endpoint>
</scheme>
</scheme>""")

# return 0 if the configuration is valid, otherwise, return 1
def arg_validate_arguments(): 
    config = sys.stdin.read()
    logging.error("Validating data from stdin: %s", config)

    try:
        doc = xml.dom.minidom.parseString(config)
        root = doc.documentElement

        item_node = root.getElementsByTagName("item")[0]
        stanza_name = item_node.getAttribute("name")
        logging.error("Validating new stanza %s"%stanza_name)
        for p in item_node.getElementsByTagName("param"):
            param_name = p.getAttribute("name")
            param_value = p.firstChild.data

            logging.error("Validating param %s=%s"%(param_name, param_value))

            if param_name == "num_events":
                num_events = int(param_value)
                if num_events < 1:
                    raise Exception("num_events cannot be negative")

        logging.error("Validation was OK")
        return 0
    except Exception as e:
        logging.error("Validation failed (%s)"%e)
        return 1


def generate_events():
    # read config data from stdin
    config = parse_config(sys.stdin.read())
    logging.error("Config from stdin: %s", config)
    
    for stanza in config["stanzas"]:
        s = config["stanzas"][stanza]
        logging.error("Generating Events for stanza: %s"%stanza)
        logging.error("stanza config: %s"%s)

        # load checkpoint data
        try:
            fname = "modinputname_"+hashlib.md5(stanza).hexdigest()
            checkpoint_file = os.path.join(config["checkpoint_dir"],fname)
            logging.error("Checkpoint filename is %s",checkpoint_file)
            checkpoint = load_checkpoint(checkpoint_file)
            logging.error("Loaded checkpoint data, %s"%checkpoint)
        except:
            pass
        
        # generate events
        try:
            last_run = 0
            events_generated = 0
            if "last_run" in checkpoint:
                last_run = checkpoint["last_run"]
            if "events_generated" in checkpoint:
                events_generated = checkpoint["events_generated"]

            num_events = int(s["num_events"])
            if num_events < 1:
                raise Exception("num_events cannot be negative")
        except Exception as e:
            logging.error("Failed to generate events (%s)"%e)
            return

        for i in range(num_events):
            now = time.time()
            now_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
            hostname = random.choice(["host1","host2","host3"])
            message = random.choice(["Test Message #1", "Test Message #2", "Test Message #3"])
            events_generated = events_generated + 1 
            print("%s %s %s Event Number %d"%(now_str, hostname, message, events_generated))

        # update and save checkpoint data
        checkpoint["last_run"] = time.time()
        checkpoint["events_generated"] = events_generated

        try:
            save_checkpoint(checkpoint_file,checkpoint)
        except Exception as e:
            logging.error("Could not save the checkpoint file %s, (%s)"%(checkpoint_file, e))
            pass


def parse_config(config_str):
    config = {}

    # https://docs.splunk.com/Documentation/Splunk/8.0.2/AdvancedDev/ModInputsScripts#Example_code_reading_XML_configuration

    try:
        # parse the config XML
        doc = xml.dom.minidom.parseString(config_str)
        root = doc.documentElement

        logging.error(config_str)

        conf_node = root.getElementsByTagName("configuration")[0]
        if conf_node:
            logging.error("XML: found configuration")

            config["stanzas"] = {}
            for stanza in conf_node.getElementsByTagName("stanza"):
                stanza_name = stanza.getAttribute("name")
                logging.error("Found Stanza: %s"%stanza_name)
                config["stanzas"][stanza_name] = {}
                for p in stanza.getElementsByTagName("param"):
                    pname = p.getAttribute("name")
                    pvalue = p.firstChild.data
                    config["stanzas"][stanza_name][pname] = pvalue

        checkpnt_node = root.getElementsByTagName("checkpoint_dir")[0]
        if checkpnt_node and checkpnt_node.firstChild and \
           checkpnt_node.firstChild.nodeType == checkpnt_node.firstChild.TEXT_NODE:
            config["checkpoint_dir"] = checkpnt_node.firstChild.data
            logging.error("Checkpoint_dir: %s"%config["checkpoint_dir"])

        if not config:
            raise Exception("Invalid configuration received from Splunk.")

    except Exception as e:
        raise Exception("Error getting Splunk configuration via STDIN: %s" % str(e))

    return config

def load_checkpoint(checkpoint_file):
    try:
        with open(checkpoint_file) as fp:
            data = json.load(fp)
            return data
    except Exception as e:
        logging.error("Error loading checkpoint file %s, (%s)"%(checkpoint_file, e))
        return {}
    return {}

def save_checkpoint(checkpoint_file,checkpoint):
    try:
        with open(checkpoint_file, 'w') as fp:
            json.dump(checkpoint, fp)
            logging.error("Saved checkpoint to Checkpoint File %s"%checkpoint_file)
    except Exception as e:
        logging.error("Error saving checkpoint to Checkpoint File, %s, (%s)"%(checkpoint_file,e))
        return 

if __name__ == '__main__':
    # setup logging
    logging.basicConfig(level=logging.DEBUG)

    # check for arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--scheme":
            arg_scheme()
        elif sys.argv[1] == "--validate-arguments":
            if arg_validate_arguments() > 0:
                sys.exit(1)
        else:
            pass
    else:
        generate_events()

    sys.exit(0)
