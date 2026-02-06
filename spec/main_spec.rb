require 'open3'

def run_script(commands)
  raw_output = nil
  Open3.popen3('./db test.db') do |stdin, stdout, stderr, wait_thr|
    commands.each { |cmd| stdin.puts cmd }
    stdin.close
    raw_output = stdout.read
  end
  raw_output.split("\n")
end

describe 'database' do
  it 'prints btree structure for 14 inserts' do
    script = (1..14).map { |i| "insert #{i} user#{i} person#{i}@example.com" }
    script << ".btree"
    script << "insert 15 user15 person15@example.com"
    script << ".exit"

    result = run_script(script)

    expect(result.last).to eq("Need to implement searching an internal node")
  end
end
