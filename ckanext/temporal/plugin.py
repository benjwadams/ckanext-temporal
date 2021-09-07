import pendulum
import datetime
import ckan.plugins as p
import ckan.plugins.toolkit as toolkit


class TemporalPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer)
    p.implements(p.IFacets)
    p.implements(p.IConfigurer)

    def before_index(self, data_dict):
        data_modified = copy.deepcopy(data_dict)
        start_end_time = []
        responsible_party = data_dict.get('extras_responsible-party')
        if responsible_party is not None:
            originators = get_originator_names(responsible_party)
            if len(originators) > 0:
                data_modified['data_provider'] = originators

        # write GCMD Keywords and CF Standard Names to corresponding solr
        # multi-index fields
        for field_name in ('cf_standard_names', 'gcmd_keywords'):
            extras_str = data_dict.get("extras_{}".format(field_name))
            if extras_str is not None:
                try:
                    extras_parse = [e.strip() for e in
                                    json.loads(extras_str)]
                except ValueError:
                    log.exception("Can't parse {} from JSON".format(field_name))
                else:
                    data_modified[field_name] = extras_parse


        # Solr StringField max length is 32766 bytes.  Truncate to this length
        # if any field exceeds this length so that harvesting doesn't crash
        max_solr_strlen_bytes = 32766
        for extra_key, extra_val in data_modified.items():
            if (extra_key not in {'data_dict', 'validated_data_dict'} and
                isinstance(extra_val, six.string_types)):
                bytes_str = extra_val.encode("utf-8")
                bytes_len = len(bytes_str)
                # TODO: if json, ignore
                if bytes_len > max_solr_strlen_bytes:
                    log.info("Key {} length of {} bytes exceeds maximum of {}, "
                             "truncating string".format(extra_key,
                                                        max_solr_strlen_bytes,
                                                        bytes_len))
                    trunc_val = bytes_str[:max_solr_strlen_bytes].decode('utf-8',
                                                                         'ignore')
                    data_modified[extra_key] = trunc_val

        log.debug(data_modified.get('temporal_extent'))
        return data_modified


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

    # time and depth aren't standard facets, so just return the regular
    # facets dict
    def dataset_facets(self, facets_dict, package_type):
        return facets_dict

    def group_facets(self, facets_dict, group_type, package_type):
        return facets_dict

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("assets", "temporal")

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

    def get_package_dict(self, context, data_dict):

        package_dict = data_dict['package_dict']
        iso_values = data_dict['iso_values']

        returned_tags =  split_gcmd_tags(iso_values['tags'])
        if returned_tags is not None:
            package_dict['tags'] = returned_tags

        # ckanext-dcat uses temporal_start and temporal_end for time extents
        # instead of temporal-extent-begin and temporal-extent-end as used by
        # CKAN
        time_pairs = (('temporal_start', 'temporal-extent-begin'),
                      ('temporal_end', 'temporal-extent-end'))
        for new_key, iso_time_field in time_pairs:
            # recreating ckanext-spatial's logic here
            if len(iso_values.get(iso_time_field, [])) > 0:
                package_dict['extras'].append(
                    {'key': new_key, 'value': iso_values[iso_time_field][0]})

        return package_dict
