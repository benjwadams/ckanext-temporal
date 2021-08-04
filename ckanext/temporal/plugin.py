import pendulum
import datetime
import ckan.plugins as p

class TemporalPlugin(p.SingletonPlugin):
    def before_search(self, search_params):
        # handle temporal filters
        if 'extras' in search_params:
            fq_modified = search_params.get('fq', '')
            extras = search_params['extras']

            if extras.get("ext_min_depth") is not None:
                vert_min = extras["ext_min_depth"]
            else:
                vert_min = "*"

            if extras.get("ext_max_depth") is not None:
                vert_max = extras["ext_max_depth"]
            else:
                vert_max = "*"

            if not (vert_min == "*" and vert_max == "*"):
                if vert_min == "*":
                    cases = "vertical_min:[* TO {}]".format(vert_max)
                elif vert_max == "*":
                    cases = "vertical_max:[{} TO *]".format(vert_min)
                # could the below expression be simplified?
                else:
                    cases = ("((vertical_min:[{0} TO {1}] AND"
                            " vertical_max:[{0} TO {1}]) OR"
                            " (vertical_min:[* TO {0}] AND"
                            " vertical_max:[{0} TO *]) OR"
                            " (vertical_min:[* TO {1}] AND"
                            " vertical_max:[{1} TO *]))").format(vert_min,
                                                                vert_max)
                fq_modified += " +{}".format(cases)

            begin_time = extras.get('ext_timerange_start')
            end_time = extras.get('ext_timerange_end')
            # if both begin and end time are none, no search window was provided
            if not (begin_time is None and end_time is None):
                try:
                    log.debug(begin_time)
                    convert_begin = convert_date(begin_time)
                    log.debug(convert_begin)
                    log.debug(end_time)
                    convert_end = convert_date(end_time)
                    log.debug(convert_end)
                except pendulum.parsing.exceptions.ParserError:
                    log.exception("Error while parsing begin/end time")
                    raise SearchError("Cannot parse provided time")


                log.debug(search_params)
                # fq should be defined in query params, but just in case, use .get
                # defaulting to empty string
                fq_modified += " +temporal_extent:[{} TO {}]".format(
                                    convert_begin, convert_end)

            search_params_modified['fq'] = fq_modified
            log.info(search_params_modified)
            return search_params_modified

def convert_date(date_val, check_datetime=False, date_to_datetime=False):
    """Given a * date or datetime string.  Optionally checks the type parsed
       of the parsed value prior to being returned as a string"""
    utc = pendulum.timezone("UTC")
    if date_val is None or date_val == '*':
        if check_datetime:
            raise ValueError("Value is not datetime")
        return '*'
    else:
        d_raw = pendulum.parsing.parse_iso8601(date_val.strip())
        if (check_datetime and not isinstance(d_raw, datetime.datetime) and
            not date_to_datetime):
            raise ValueError("Value is not datetime")
        if isinstance(d_raw, datetime.datetime):
            pendulum_date = utc.convert(pendulum.instance(d_raw))
            # need to truncate/eliminate microseconds in order to work with solr
            if pendulum_date.microsecond == 0:
               return pendulum_date.to_iso8601_string()
            else:
                log.info("Datetime has nonzero microseconds, truncating to "
                         "zero for compatibility with Solr")
                return pendulum_date.replace(microsecond=0).to_iso8601_string()
        # if not a datetime, then it's a date
        elif isinstance(d_raw, datetime.date):
            if date_to_datetime:
                # any more elegant way to achieve conversion to datetime?
                dt_force = datetime.datetime.combine(d_raw,
                                                datetime.datetime.min.time())
                # probably don't strictly need tz argument, but doesn't hurt
                # to be explicit
                new_dt_str = pendulum.instance(dt_force,
                                               tz=utc).to_iso8601_string()
                log.info("Converted date {} to datetime {}".format(
                            d_raw.isoformat(), new_dt_str))
                return new_dt_str
            else:
               return d_raw.isoformat()

        else:
            # probably won't reach here, but not a bad idea to be defensive anyhow
            raise ValueError("Type {} is not handled by the datetime conversion routine")
