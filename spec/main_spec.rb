require 'open3'

def run_script(commands)
  raw_output = nil
  Open3.popen3('./db test.db') do |stdin, stdout, stderr, wait_thr|
    commands.each do |cmd|
      begin
        stdin.puts cmd
      rescue Errno::EPIPE
        break
      end
    end
    stdin.close
    raw_output = stdout.read
  end
  raw_output.split("\n")
end

describe 'database' do
  it 'handles inserting 15 rows without internal search error' do
    script = (1..15).map { |i| "insert #{i} user#{i} person#{i}@example.com" }
    script << ".exit"
    result = run_script(script)
    expect(result).to include("db > Executed.")
  end

  it 'eventually errors on deeper split' do
    script = (1..1400).map { |i| "insert #{i} u e" }
    script << ".exit"
    result = run_script(script)

    expect(result.last(2)).to match_array([
      "db > Executed.",
      "db > Need to implement updating parent after split"
    ])
  end
end
