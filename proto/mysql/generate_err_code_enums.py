import gflags
import sys


gflags.DEFINE_string('mysql_errmsg_utf8_file',
                     None,
                     'The path to the error message file.  The file is usually '
                     'located at <mysql_src_root>/sql/share/errmsg-utf8.txt')
gflags.DEFINE_string('output_proto_file',
                     None,
                     'Where to write the output to.')
gflags.DEFINE_string('proto_package_name',
                     'mysql',
                     'The output proto package name.')

FLAGS = gflags.FLAGS

ERROR_PREFIX = 'ER_'
WARNING_PREFIX = 'WARN_'

PROTO_TEMPLATE = """package %s;
// AUTO-GENERATED.  DO NOT MODIFY!
//
// original command: %s
//
// For additional information on error codes, see
// https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html

message ErrorCode {
    enum Type {
        OK = 0;
%s
    }
}"""

def parse_options():
    def parse_error(e):
        print '%s\n\nUsage: %s' % (e, FLAGS)
        sys.exit(1)

    try:
        FLAGS(sys.argv)
    except gflags.FlagsError, e:
        parse_error(e)

    if not FLAGS.mysql_errmsg_utf8_file:
        parse_error('--mysql_errmsg_utf8_file is not specified.')

    if not FLAGS.output_proto_file:
        parse_error('--output_proto_file is not specified.')


def main():
    parse_options()

    codes = []
    for l in open(FLAGS.mysql_errmsg_utf8_file):
        l = l.strip()

        if l.startswith('start-error-number'):
            start_error_number = int(l.split()[1])
            assert start_error_number == 1000
            continue

        if l.startswith(ERROR_PREFIX) or l.startswith(WARNING_PREFIX):
            codes.append(l.split()[0])

    entries = []
    for i, c in enumerate(codes):
        entries.append('        %s = %s;' % (c, 1000 + i))

    fd = open(FLAGS.output_proto_file, 'w')
    fd.write(PROTO_TEMPLATE % (' '.join(sys.argv),
                               FLAGS.proto_package_name,
                               '\n'.join(entries)))
    fd.close()

if __name__ == '__main__':
    main()
